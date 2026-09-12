use crate::chain::USER_AGENT;
use crate::config::Config;
use crate::rpc::{LatencyRecorder, LatencyStats};
use alloy::primitives::{B256, Bytes};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct SequencerIp {
    pub ip: String,
    pub rtt_ms: u64,
}

#[derive(Clone)]
struct PinnedSequencer {
    stat: SequencerIp,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub enum SendOutcome {
    Hash(B256),
    Known,
    Revert { message: String },
    Reject { code: i64, message: String },
    Error(String),
}

/// Warm HTTP/1.1 pool to the sequencer. Alloy is not used for send.
pub struct Submitter {
    url: String,
    host: String,
    pinned: parking_lot::RwLock<Vec<PinnedSequencer>>,
    client: reqwest::Client,
    next: AtomicU64,
    latency: LatencyRecorder,
    fallback: Option<reqwest::Client>,
    fallback_url: Option<String>,
}

impl Submitter {
    pub fn new(cfg: &Config) -> anyhow::Result<Self> {
        let url = cfg.sequencer_url.clone();
        let host = url::Url::parse(&url)?
            .host_str()
            .unwrap_or("sequencer.mainnet.chain.robinhood.com")
            .to_string();
        let fallback_url = cfg.rpc_http.iter().find(|e| e.logs).map(|e| e.url.clone());
        Ok(Self {
            url,
            host,
            pinned: parking_lot::RwLock::new(vec![]),
            client: build_client()?,
            next: AtomicU64::new(0),
            latency: LatencyRecorder::default(),
            fallback: Some(build_client()?),
            fallback_url,
        })
    }

    pub async fn resolve_and_pin(&self) -> anyhow::Result<Vec<SequencerIp>> {
        let addrs = tokio::net::lookup_host((self.host.as_str(), 443))
            .await?
            .collect::<Vec<_>>();
        let mut endpoints = Vec::new();
        let mut failures = Vec::new();
        for address in addrs {
            let ip = match address {
                SocketAddr::V4(value) => value.ip().to_string(),
                SocketAddr::V6(value) => value.ip().to_string(),
            };
            if endpoints
                .iter()
                .any(|endpoint: &PinnedSequencer| endpoint.stat.ip == ip)
            {
                continue;
            }
            let client = client_pinned(&self.host, &ip)?;
            match warmup_rtt(&client, &self.url, &self.host, &ip).await {
                Ok(rtt_ms) => endpoints.push(PinnedSequencer {
                    stat: SequencerIp { ip, rtt_ms },
                    client,
                }),
                Err(error) => failures.push(format!("{ip}: {error}")),
            }
        }
        anyhow::ensure!(
            !endpoints.is_empty(),
            "no sequencer address completed a pinned HTTP warm-up: {}",
            failures.join("; ")
        );
        endpoints.sort_by_key(|endpoint| endpoint.stat.rtt_ms);
        let stats = endpoints
            .iter()
            .map(|endpoint| endpoint.stat.clone())
            .collect();
        *self.pinned.write() = endpoints;
        Ok(stats)
    }

    fn next_client(&self) -> reqwest::Client {
        let pinned = self.pinned.read();
        if pinned.is_empty() {
            return self.client.clone();
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) as usize % pinned.len();
        pinned[index].client.clone()
    }

    pub fn latency(&self) -> LatencyStats {
        self.latency.stats()
    }

    pub async fn send_raw(&self, raw: &Bytes) -> SendOutcome {
        let started = Instant::now();
        let hex = format!("0x{}", hex::encode(raw));
        let client = self.next_client();
        let outcome = match self
            .rpc_send(
                &client,
                &self.url,
                "eth_sendRawTransaction",
                json!([hex.clone()]),
            )
            .await
        {
            Ok(value) => parse_send(value),
            Err(SendOutcome::Error(error)) => {
                // Transport-level failure — worth one try on the public RPC.
                // A sequencer-side rejection (revert/known/nonce) is a decision,
                // not a connection problem, so it is NOT retried elsewhere.
                if let (Some(client), Some(url)) = (&self.fallback, &self.fallback_url) {
                    match self
                        .rpc_send(client, url, "eth_sendRawTransaction", json!([hex]))
                        .await
                    {
                        Ok(value) => parse_send(value),
                        Err(SendOutcome::Error(fallback)) => {
                            SendOutcome::Error(format!("{error}; fallback {fallback}"))
                        }
                        Err(other) => other,
                    }
                } else {
                    SendOutcome::Error(error)
                }
            }
            Err(other) => other,
        };
        self.latency.record(started.elapsed());
        outcome
    }

    /// Spray the same nonce to every known sequencer IP. First sequenced wins.
    pub async fn spray(&self, raw: &Bytes) -> SendOutcome {
        let endpoints = self.pinned.read().clone();
        if endpoints.len() <= 1 {
            return self.send_raw(raw).await;
        }
        let started = Instant::now();
        let hex = format!("0x{}", hex::encode(raw));
        let mut futures = Vec::new();
        for endpoint in endpoints {
            let url = self.url.clone();
            let host = self.host.clone();
            let hex = hex.clone();
            futures.push(async move { pinned_send(&endpoint.client, &url, &host, &hex).await });
        }
        let outcome = best_send(futures_util::future::join_all(futures).await);
        self.latency.record(started.elapsed());
        outcome
    }

    pub async fn send_conditional(&self, raw: &Bytes, opts: Value) -> Result<Value, (i64, String)> {
        match self
            .rpc(
                &self.client,
                &self.url,
                "eth_sendRawTransactionConditional",
                json!([format!("0x{}", hex::encode(raw)), opts]),
            )
            .await
        {
            Ok(v) => Ok(v),
            Err(e) => Err(parse_rpc_err(&e.to_string())),
        }
    }

    /// Like `rpc` but keeps the failure classification: transport problems are
    /// `SendOutcome::Error`, JSON-RPC rejections are decoded into Known/Revert/
    /// Reject so callers can distinguish "not sent" from "refused".
    async fn rpc_send(
        &self,
        client: &reqwest::Client,
        url: &str,
        method: &str,
        params: Value,
    ) -> Result<Value, SendOutcome> {
        let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        let res = client
            .post(url)
            .header("user-agent", USER_AGENT)
            .header("content-type", "application/json")
            .header("host", &self.host)
            .json(&body)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| SendOutcome::Error(e.to_string()))?;
        let v: Value = res
            .json()
            .await
            .map_err(|e| SendOutcome::Error(e.to_string()))?;
        if let Some(err) = v.get("error") {
            return Err(classify_send_err(&err.to_string()));
        }
        Ok(v.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn rpc(
        &self,
        client: &reqwest::Client,
        url: &str,
        method: &str,
        params: Value,
    ) -> anyhow::Result<Value> {
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

    pub fn spawn_refresh(self: &std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
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
        })
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
    let addr: SocketAddr = format!("{ip}:443")
        .parse()
        .or_else(|_| format!("[{ip}]:443").parse())?;
    Ok(reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .resolve(host, addr)
        .pool_max_idle_per_host(4)
        .tcp_nodelay(true)
        .http1_only()
        .timeout(Duration::from_secs(15))
        .build()?)
}

async fn warmup_rtt(
    client: &reqwest::Client,
    url: &str,
    host: &str,
    ip: &str,
) -> anyhow::Result<u64> {
    let t0 = Instant::now();
    let body = json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]});
    let response = client
        .post(url)
        .header("host", host)
        .header("user-agent", USER_AGENT)
        .json(&body)
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "sequencer {ip}: HTTP {}",
        response.status()
    );
    let value: Value = response.json().await?;
    anyhow::ensure!(
        value.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
            && (value.get("error").is_some() || value.get("result").is_some()),
        "sequencer {ip}: invalid JSON-RPC response"
    );
    Ok(t0.elapsed().as_millis() as u64)
}

async fn pinned_send(client: &reqwest::Client, url: &str, host: &str, hex: &str) -> SendOutcome {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":[hex]});
    match client
        .post(url)
        .header("host", host)
        .header("user-agent", USER_AGENT)
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await
    {
        Ok(res) => match res.json::<Value>().await {
            Ok(v) => {
                if let Some(err) = v.get("error") {
                    return classify_send_err(&err.to_string());
                }
                parse_send(v.get("result").cloned().unwrap_or(v))
            }
            Err(e) => SendOutcome::Error(e.to_string()),
        },
        Err(e) => SendOutcome::Error(e.to_string()),
    }
}

fn best_send(results: Vec<SendOutcome>) -> SendOutcome {
    results
        .iter()
        .find_map(|outcome| match outcome {
            SendOutcome::Hash(hash) => Some(SendOutcome::Hash(*hash)),
            _ => None,
        })
        .or_else(|| {
            results
                .iter()
                .any(|outcome| matches!(outcome, SendOutcome::Known))
                .then_some(SendOutcome::Known)
        })
        .or_else(|| {
            results
                .into_iter()
                .find(|outcome| matches!(outcome, SendOutcome::Revert { .. }))
        })
        .unwrap_or_else(|| SendOutcome::Error("all sprays failed".into()))
}

fn parse_send(v: Value) -> SendOutcome {
    if let Some(s) = v.as_str()
        && s.starts_with("0x")
        && s.len() == 66
        && let Ok(h) = s.parse()
    {
        return SendOutcome::Hash(h);
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

/// Map a JSON-RPC error body to a send classification. `Known` means the nonce
/// slot is spoken for (our tx or a same-nonce sibling) — reconcile by receipt,
/// never by this label.
pub fn classify_send_err(text: &str) -> SendOutcome {
    let (code, msg) = parse_rpc_err(text);
    let m = msg.to_ascii_lowercase();
    if m.contains("already known")
        || m.contains("already_exists")
        || m.contains("nonce too low")
        || m.contains("replacement")
    {
        return SendOutcome::Known;
    }
    if m.contains("revert") {
        return SendOutcome::Revert { message: msg };
    }
    SendOutcome::Reject { code, message: msg }
}

fn parse_rpc_err(s: &str) -> (i64, String) {
    if let Ok(v) = serde_json::from_str::<Value>(s) {
        let code = v.get("code").and_then(|c| c.as_i64()).unwrap_or(-32000);
        let msg = v
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or(s)
            .to_string();
        return (code, msg);
    }
    if s.contains("-32003") {
        return (-32003, s.to_string());
    }
    (-32000, s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spray_prefers_acceptance_over_a_faster_revert() {
        let hash = B256::from([1u8; 32]);
        let result = best_send(vec![
            SendOutcome::Revert {
                message: "stale replica".into(),
            },
            SendOutcome::Hash(hash),
        ]);
        assert!(matches!(result, SendOutcome::Hash(value) if value == hash));
    }

    #[test]
    fn send_errors_keep_their_safety_classification() {
        assert!(matches!(
            classify_send_err(r#"{"code":-32000,"message":"already known"}"#),
            SendOutcome::Known
        ));
        assert!(matches!(
            classify_send_err(r#"{"code":-32000,"message":"execution reverted"}"#),
            SendOutcome::Revert { .. }
        ));
        assert!(matches!(
            classify_send_err(r#"{"code":-32003,"message":"precheck rejected"}"#),
            SendOutcome::Reject { code: -32003, .. }
        ));
    }
}
