use crate::chain::{DEFAULT_SEQUENCER, USER_AGENT};
use crate::config::Config;
use alloy::primitives::{Bytes, B256};
use serde_json::{json, Value};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct SequencerIp {
    pub ip: String,
    pub rtt_ms: u64,
}

#[derive(Debug, Clone)]
pub enum SendOutcome {
    Hash(B256),
    Known,
    Revert { hash: Option<B256>, message: String },
    Reject { code: i64, message: String },
    Error(String),
}

/// Warm HTTP/1.1 pool to the sequencer. Alloy is not used for send.
pub struct Submitter {
    url: String,
    host: String,
    pinned: parking_lot::RwLock<Vec<SequencerIp>>,
    clients: Vec<reqwest::Client>,
    next: AtomicU64,
    fallback: Option<reqwest::Client>,
    fallback_url: Option<String>,
}

impl Submitter {
    pub fn new(cfg: &Config) -> anyhow::Result<Self> {
        let url = cfg.sequencer_url.clone();
        let host = url::Url::parse(&url)?.host_str().unwrap_or("sequencer.mainnet.chain.robinhood.com").to_string();
        let n = crate::config::env_u64("BURST_CONNS", 12).clamp(4, 32) as usize;
        let mut clients = Vec::with_capacity(n);
        for _ in 0..n {
            clients.push(build_client()?);
        }
        let fallback_url = cfg.rpc_http.iter().find(|e| e.logs).map(|e| e.url.clone());
        Ok(Self {
            url,
            host,
            pinned: parking_lot::RwLock::new(vec![]),
            clients,
            next: AtomicU64::new(0),
            fallback: Some(build_client()?),
            fallback_url,
        })
    }

    pub async fn resolve_and_pin(&self) -> anyhow::Result<Vec<SequencerIp>> {
        let host = self.host.clone();
        let addrs = tokio::task::spawn_blocking(move || (host.as_str(), 443).to_socket_addrs().map(|i| i.collect::<Vec<_>>()))
            .await??;
        let mut ips = Vec::new();
        for a in addrs {
            let ip = match a {
                SocketAddr::V4(v) => v.ip().to_string(),
                SocketAddr::V6(v) => v.ip().to_string(),
            };
            if ips.iter().any(|x: &SequencerIp| x.ip == ip) {
                continue;
            }
            let rtt = warmup_rtt(&self.url, &self.host, &ip).await.unwrap_or(9_999);
            ips.push(SequencerIp { ip, rtt_ms: rtt });
        }
        ips.sort_by_key(|i| i.rtt_ms);
        *self.pinned.write() = ips.clone();
        Ok(ips)
    }

    pub fn best_ip(&self) -> Option<SequencerIp> {
        self.pinned.read().first().cloned()
    }

    pub fn all_ips(&self) -> Vec<SequencerIp> {
        self.pinned.read().clone()
    }

    fn client(&self) -> &reqwest::Client {
        let i = self.next.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len();
        &self.clients[i]
    }

    pub async fn send_raw(&self, raw: &Bytes) -> SendOutcome {
        match self.rpc(self.client(), &self.url, "eth_sendRawTransaction", json!([format!("0x{}", hex::encode(raw))])).await {
            Ok(v) => parse_send(v),
            Err(e) => {
                if let (Some(c), Some(u)) = (&self.fallback, &self.fallback_url) {
                    match self.rpc(c, u, "eth_sendRawTransaction", json!([format!("0x{}", hex::encode(raw))])).await {
                        Ok(v) => parse_send(v),
                        Err(e2) => SendOutcome::Error(format!("{e}; fallback {e2}")),
                    }
                } else {
                    SendOutcome::Error(e.to_string())
                }
            }
        }
    }

    /// Spray the same nonce to every known sequencer IP. First sequenced wins.
    pub async fn spray(&self, raw: &Bytes) -> SendOutcome {
        let ips = self.all_ips();
        if ips.len() <= 1 {
            return self.send_raw(raw).await;
        }
        let hex = format!("0x{}", hex::encode(raw));
        let mut futs = Vec::new();
        for ip in &ips {
            let url = self.url.clone();
            let host = self.host.clone();
            let hex = hex.clone();
            let ip = ip.ip.clone();
            futs.push(async move { pinned_send(&url, &host, &ip, &hex).await });
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            match r {
                SendOutcome::Hash(h) => return SendOutcome::Hash(h),
                SendOutcome::Known => return SendOutcome::Known,
                other => {
                    if matches!(other, SendOutcome::Revert { .. }) {
                        return other;
                    }
                }
            }
        }
        SendOutcome::Error("all sprays failed".into())
    }

    /// Confirmation only, after a fill. Timeout is a 0x-hex quantity.
    pub async fn send_raw_sync(&self, raw: &Bytes, timeout_hex: &str) -> anyhow::Result<Value> {
        self.rpc(
            self.client(),
            &self.url,
            "eth_sendRawTransactionSync",
            json!([format!("0x{}", hex::encode(raw)), timeout_hex]),
        )
        .await
    }

    pub async fn send_conditional(&self, raw: &Bytes, opts: Value) -> Result<Value, (i64, String)> {
        match self
            .rpc(self.client(), &self.url, "eth_sendRawTransactionConditional", json!([format!("0x{}", hex::encode(raw)), opts]))
            .await
        {
            Ok(v) => Ok(v),
            Err(e) => Err(parse_rpc_err(&e.to_string())),
        }
    }

    async fn rpc(&self, client: &reqwest::Client, url: &str, method: &str, params: Value) -> anyhow::Result<Value> {
        let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        let res = client
            .post(url)
            .header("user-agent", USER_AGENT)
            .header("content-type", "application/json")
            .header("host", &self.host)
            .json(&body)
            .timeout(Duration::from_secs(15))
            .send()
            .await?;
        let v: Value = res.json().await?;
        if let Some(err) = v.get("error") {
            anyhow::bail!("{}", err);
        }
        Ok(v.get("result").cloned().unwrap_or(Value::Null))
    }

    pub fn spawn_refresh(self: &std::sync::Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(300));
            iv.tick().await;
            loop {
                iv.tick().await;
                if let Err(e) = this.resolve_and_pin().await {
                    tracing::debug!("sequencer re-resolve: {e}");
                }
            }
        });
    }

    pub async fn ping(&self) -> anyhow::Result<u64> {
        let t0 = Instant::now();
        let _ = self.rpc(self.client(), &self.url, "eth_chainId", json!([])).await;
        Ok(t0.elapsed().as_millis() as u64)
    }
}

fn build_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .pool_max_idle_per_host(4)
        .tcp_nodelay(true)
        .http1_only()
        .timeout(Duration::from_secs(15))
        .build()?)
}

fn client_pinned(host: &str, ip: &str) -> anyhow::Result<reqwest::Client> {
    let addr: SocketAddr = format!("{ip}:443").parse().or_else(|_| format!("[{ip}]:443").parse())?;
    Ok(reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .resolve(host, addr)
        .pool_max_idle_per_host(4)
        .tcp_nodelay(true)
        .http1_only()
        .timeout(Duration::from_secs(15))
        .build()?)
}

async fn warmup_rtt(url: &str, host: &str, ip: &str) -> anyhow::Result<u64> {
    let client = client_pinned(host, ip)?;
    let t0 = Instant::now();
    let body = json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]});
    let _ = client.post(url).header("host", host).header("user-agent", USER_AGENT).json(&body).send().await;
    Ok(t0.elapsed().as_millis() as u64)
}

async fn pinned_send(url: &str, host: &str, ip: &str, hex: &str) -> SendOutcome {
    let client = match client_pinned(host, ip) {
        Ok(c) => c,
        Err(e) => return SendOutcome::Error(e.to_string()),
    };
    let body = json!({"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":[hex]});
    match client.post(url).header("host", host).header("user-agent", USER_AGENT).json(&body).timeout(Duration::from_secs(15)).send().await {
        Ok(res) => match res.json::<Value>().await {
            Ok(v) => {
                if let Some(err) = v.get("error") {
                    return parse_send(err.clone());
                }
                parse_send(v.get("result").cloned().unwrap_or(v))
            }
            Err(e) => SendOutcome::Error(e.to_string()),
        },
        Err(e) => SendOutcome::Error(e.to_string()),
    }
}

fn parse_send(v: Value) -> SendOutcome {
    if let Some(s) = v.as_str() {
        if s.starts_with("0x") && s.len() == 66 {
            if let Ok(h) = s.parse() {
                return SendOutcome::Hash(h);
            }
        }
    }
    let t = v.to_string();
    if t.contains("already known") || t.contains("ALREADY_EXISTS") {
        return SendOutcome::Known;
    }
    if t.contains("nonce too low") || t.contains("replacement") {
        return SendOutcome::Known;
    }
    SendOutcome::Error(t)
}

fn parse_rpc_err(s: &str) -> (i64, String) {
    if let Ok(v) = serde_json::from_str::<Value>(s) {
        let code = v.get("code").and_then(|c| c.as_i64()).unwrap_or(-32000);
        let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or(s).to_string();
        return (code, msg);
    }
    if s.contains("-32003") {
        return (-32003, s.to_string());
    }
    (-32000, s.to_string())
}

#[allow(dead_code)]
fn _unused() {
    let _ = DEFAULT_SEQUENCER;
}
