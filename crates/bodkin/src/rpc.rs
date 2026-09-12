use crate::chain::USER_AGENT;
use crate::config::{Config, EndpointCfg};
use crate::pons::clock::HeaderView;
use alloy::primitives::{Address, Bytes, B256, U256};
use alloy::rpc::types::{Block, Filter, Log, Transaction, TransactionReceipt};
use alloy::sol_types::{SolCall, SolValue};
use futures::{Stream, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Hot,
    Enrich,
    Background,
}

#[derive(Debug, Clone)]
pub struct GateStats {
    pub active: u64,
    pub queued: usize,
    pub in_flight: usize,
    pub spacing_ms: u64,
    pub logs_spacing_ms: u64,
    pub throttled: u64,
    pub cooling_down: bool,
    pub endpoints: Vec<EpStat>,
}

#[derive(Debug, Clone)]
pub struct EpStat {
    pub label: String,
    pub logs: bool,
    pub benched: bool,
}

struct Endpoint {
    cfg: EndpointCfg,
    bad_until: AtomicU64,
    client: reqwest::Client,
}

struct Gate {
    sem: Semaphore,
    last_start: Mutex<Instant>,
    last_logs: Mutex<Instant>,
    cooldown_until: AtomicU64,
    throttled: AtomicU64,
    spacing_ms: u64,
    logs_spacing_ms: u64,
    in_flight: usize,
}

pub struct Rpc {
    endpoints: Vec<Arc<Endpoint>>,
    gate: Arc<Gate>,
    next_id: AtomicU64,
}

impl Rpc {
    pub fn new(cfg: &Config) -> anyhow::Result<Self> {
        let client = |timeout: u64| {
            reqwest::Client::builder()
                .user_agent(USER_AGENT)
                .timeout(Duration::from_millis(timeout))
                .tcp_nodelay(true)
                .pool_max_idle_per_host(8)
                .build()
        };
        let endpoints = cfg
            .rpc_http
            .iter()
            .map(|e| {
                Ok(Arc::new(Endpoint {
                    cfg: e.clone(),
                    bad_until: AtomicU64::new(0),
                    client: client(20_000)?,
                }))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            endpoints,
            gate: Arc::new(Gate {
                sem: Semaphore::new(cfg.rpc_in_flight.max(1)),
                last_start: Mutex::new(Instant::now() - Duration::from_secs(1)),
                last_logs: Mutex::new(Instant::now() - Duration::from_secs(1)),
                cooldown_until: AtomicU64::new(0),
                throttled: AtomicU64::new(0),
                spacing_ms: cfg.rpc_spacing_ms,
                logs_spacing_ms: cfg.rpc_logs_spacing_ms,
                in_flight: cfg.rpc_in_flight,
            }),
            next_id: AtomicU64::new(1),
        })
    }

    pub fn stats(&self) -> GateStats {
        let now = now_ms();
        GateStats {
            active: (self.gate.in_flight as u64).saturating_sub(self.gate.sem.available_permits() as u64),
            queued: 0,
            in_flight: self.gate.in_flight,
            spacing_ms: self.gate.spacing_ms,
            logs_spacing_ms: self.gate.logs_spacing_ms,
            throttled: self.gate.throttled.load(Ordering::Relaxed),
            cooling_down: self.gate.cooldown_until.load(Ordering::Relaxed) > now,
            endpoints: self
                .endpoints
                .iter()
                .map(|e| EpStat {
                    label: e.cfg.label.clone(),
                    logs: e.cfg.logs,
                    benched: e.bad_until.load(Ordering::Relaxed) > now,
                })
                .collect(),
        }
    }

    pub async fn call(&self, lane: Lane, method: &str, params: Value) -> anyhow::Result<Value> {
        let _ = lane;
        let mut last = "no endpoint".to_string();
        for attempt in 0..8u32 {
            let list = self.candidates(method);
            if list.is_empty() {
                anyhow::bail!("rpc {method}: no configured endpoint serves this method");
            }
            let ep = list[attempt as usize % list.len()].clone();
            self.acquire(method).await;
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let body = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
            let res = ep.client.post(&ep.cfg.url).header("content-type", "application/json").json(&body).send().await;
            self.release();
            match res {
                Ok(r) => {
                    let status = r.status().as_u16();
                    let text = r.text().await.unwrap_or_default();
                    if status == 429 || status == 503 {
                        self.gate.throttled.fetch_add(1, Ordering::Relaxed);
                        self.gate.cooldown_until.store(now_ms() + 3_000, Ordering::Relaxed);
                        ep.bad_until.store(now_ms() + 4_000, Ordering::Relaxed);
                        last = format!("{}: HTTP {status}", ep.cfg.label);
                        tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                        continue;
                    }
                    if status == 403 && text.to_ascii_lowercase().contains("cloudflare") {
                        ep.bad_until.store(now_ms() + 60_000, Ordering::Relaxed);
                        last = format!("{}: bot-protection challenge", ep.cfg.label);
                        continue;
                    }
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => {
                            last = format!("{}: HTTP {status} non-JSON", ep.cfg.label);
                            continue;
                        }
                    };
                    if let Some(err) = v.get("error") {
                        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
                        if code == 429 {
                            self.gate.throttled.fetch_add(1, Ordering::Relaxed);
                            last = format!("{}: 429", ep.cfg.label);
                            continue;
                        }
                        anyhow::bail!("{}", err.get("message").and_then(|m| m.as_str()).unwrap_or(&err.to_string()));
                    }
                    return Ok(v.get("result").cloned().unwrap_or(Value::Null));
                }
                Err(e) => {
                    last = format!("{}: {e}", ep.cfg.label);
                    ep.bad_until.store(now_ms() + 5_000, Ordering::Relaxed);
                }
            }
        }
        anyhow::bail!("rpc {method}: gave up ({last})")
    }

    /// Race the same read across healthy endpoints; first success wins.
    pub async fn race(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let list = self.candidates(method);
        if list.is_empty() {
            anyhow::bail!("no endpoints");
        }
        let next_id = &self.next_id;
        let mut futs = list
            .into_iter()
            .map(|ep| {
                let method = method.to_string();
                let params = params.clone();
                async move {
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let body = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
                    let r = ep.client.post(&ep.cfg.url).header("content-type", "application/json").json(&body).send().await?;
                    let v: Value = r.json().await?;
                    if v.get("error").is_some() {
                        anyhow::bail!("rpc error");
                    }
                    Ok::<Value, anyhow::Error>(v.get("result").cloned().unwrap_or(Value::Null))
                }
            })
            .collect::<Vec<_>>();
        if futs.len() == 1 {
            return futs.remove(0).await;
        }
        let a = futs.remove(0);
        let b = futs.remove(0);
        tokio::select! {
            r = a => r,
            r = b => r,
        }
    }

    pub async fn block_number(&self, lane: Lane) -> anyhow::Result<u64> {
        let v = self.call(lane, "eth_blockNumber", json!([])).await?;
        hex_u64(&v)
    }

    pub async fn chain_id(&self, lane: Lane) -> anyhow::Result<u64> {
        hex_u64(&self.call(lane, "eth_chainId", json!([])).await?)
    }

    pub async fn gas_price(&self, lane: Lane) -> anyhow::Result<U256> {
        hex_u256(&self.call(lane, "eth_gasPrice", json!([])).await?)
    }

    pub async fn get_balance(&self, lane: Lane, addr: Address) -> anyhow::Result<U256> {
        hex_u256(&self.call(lane, "eth_getBalance", json!([format!("{addr:#x}"), "latest"])).await?)
    }

    pub async fn get_code(&self, lane: Lane, addr: Address) -> anyhow::Result<Bytes> {
        let v = self.call(lane, "eth_getCode", json!([format!("{addr:#x}"), "latest"])).await?;
        let s = v.as_str().unwrap_or("0x");
        Ok(Bytes::from(hex::decode(s.trim_start_matches("0x")).unwrap_or_default()))
    }

    pub async fn get_transaction_count(&self, lane: Lane, addr: Address, pending: bool) -> anyhow::Result<u64> {
        let tag = if pending { "pending" } else { "latest" };
        hex_u64(&self.call(lane, "eth_getTransactionCount", json!([format!("{addr:#x}"), tag])).await?)
    }

    pub async fn eth_call<C: SolCall>(&self, lane: Lane, to: Address, call: C, from: Option<Address>) -> anyhow::Result<C::Return> {
        let data = format!("0x{}", hex::encode(call.abi_encode()));
        let mut obj = json!({"to": format!("{to:#x}"), "data": data});
        if let Some(f) = from {
            obj["from"] = json!(format!("{f:#x}"));
        }
        let v = self.call(lane, "eth_call", json!([obj, "latest"])).await?;
        let s = v.as_str().ok_or_else(|| anyhow::anyhow!("eth_call non-hex"))?;
        let bytes = hex::decode(s.trim_start_matches("0x"))?;
        C::abi_decode_returns(&bytes).map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn eth_call_raw(&self, lane: Lane, to: Address, data: &[u8]) -> anyhow::Result<Bytes> {
        let obj = json!({"to": format!("{to:#x}"), "data": format!("0x{}", hex::encode(data))});
        let v = self.call(lane, "eth_call", json!([obj, "latest"])).await?;
        let s = v.as_str().unwrap_or("0x");
        Ok(Bytes::from(hex::decode(s.trim_start_matches("0x")).unwrap_or_default()))
    }

    pub async fn multicall3(&self, lane: Lane, calls: &[(Address, Vec<u8>)]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        use crate::abi::multicall3::{aggregate3Call, Call3};
        use crate::chain::MULTICALL3;
        let encoded = aggregate3Call {
            calls: calls
                .iter()
                .map(|(t, d)| Call3 { target: *t, allowFailure: true, callData: Bytes::from(d.clone()) })
                .collect(),
        }
        .abi_encode();
        let raw = self.eth_call_raw(lane, MULTICALL3, &encoded).await?;
        let decoded = aggregate3Call::abi_decode_returns(&raw)?;
        Ok(decoded.into_iter().map(|r| if r.success { Some(r.returnData.to_vec()) } else { None }).collect())
    }

    pub async fn get_logs(&self, lane: Lane, filter: Filter) -> anyhow::Result<Vec<Log>> {
        let v = self.call(lane, "eth_getLogs", json!([filter_to_json(&filter)])).await?;
        Ok(serde_json::from_value(v)?)
    }

    pub async fn get_transaction(&self, lane: Lane, hash: B256) -> anyhow::Result<crate::pons::enrich::TxView> {
        let v = self.call(lane, "eth_getTransactionByHash", json!([format!("{hash:#x}")])).await?;
        let from = v.get("from").and_then(|x| x.as_str()).unwrap_or_default().parse().unwrap_or(Address::ZERO);
        let to = v.get("to").and_then(|x| x.as_str()).and_then(|s| s.parse().ok());
        let value = hex_u256(v.get("value").unwrap_or(&Value::Null)).unwrap_or(U256::ZERO);
        let input = {
            let s = v.get("input").and_then(|x| x.as_str()).unwrap_or("0x");
            Bytes::from(hex::decode(s.trim_start_matches("0x")).unwrap_or_default())
        };
        let _ = std::any::type_name::<Transaction>();
        Ok(crate::pons::enrich::TxView { from, to, value, input })
    }

    pub async fn get_transaction_receipt(&self, lane: Lane, hash: B256) -> anyhow::Result<crate::pons::enrich::ReceiptView> {
        let v = self.call(lane, "eth_getTransactionReceipt", json!([format!("{hash:#x}")])).await?;
        let logs: Vec<Log> = serde_json::from_value(v.get("logs").cloned().unwrap_or(Value::Array(vec![]))).unwrap_or_default();
        let ts = v.get("blockTimestamp").map(hex_u64).transpose().ok().flatten();
        let _ = std::any::type_name::<TransactionReceipt>();
        Ok(crate::pons::enrich::ReceiptView { logs, block_timestamp: ts })
    }

    pub async fn latest_header(&self, lane: Lane) -> anyhow::Result<HeaderView> {
        let v = self.call(lane, "eth_getBlockByNumber", json!(["latest", false])).await?;
        let n = hex_u64(v.get("number").unwrap_or(&Value::Null))?;
        let ts = hex_u64(v.get("timestamp").unwrap_or(&Value::Null))?;
        let bf = v.get("baseFeePerGas").map(hex_u64).transpose()?.unwrap_or(0);
        Ok(HeaderView { number: n, timestamp: ts, base_fee: bf })
    }

    pub async fn block_timestamp(&self, lane: Lane, number: u64) -> anyhow::Result<u64> {
        let v = self.call(lane, "eth_getBlockByNumber", json!([format!("0x{number:x}"), false])).await?;
        hex_u64(v.get("timestamp").unwrap_or(&Value::Null))
    }

    pub async fn get_block(&self, lane: Lane, number: u64) -> anyhow::Result<Block> {
        let v = self.call(lane, "eth_getBlockByNumber", json!([format!("0x{number:x}"), false])).await?;
        Ok(serde_json::from_value(v)?)
    }

    pub async fn subscribe_logs(&self, ws_url: &str, filter: Filter) -> anyhow::Result<impl Stream<Item = anyhow::Result<Log>> + Send> {
        use alloy::providers::{Provider, ProviderBuilder, WsConnect};
        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new().disable_recommended_fillers().connect_ws(ws).await?;
        let sub = provider.subscribe_logs(&filter).await?;
        Ok(sub.into_stream().map(Ok))
    }

    pub async fn subscribe_heads(&self, ws_url: &str) -> anyhow::Result<impl Stream<Item = anyhow::Result<HeaderView>> + Send> {
        use alloy::providers::{Provider, ProviderBuilder, WsConnect};
        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new().disable_recommended_fillers().connect_ws(ws).await?;
        let sub = provider.subscribe_blocks().await?;
        Ok(sub.into_stream().map(|h| {
            Ok(HeaderView {
                number: h.number,
                timestamp: h.timestamp,
                base_fee: h.base_fee_per_gas.unwrap_or(0),
            })
        }))
    }

    async fn acquire(&self, method: &str) {
        let _ = self.gate.sem.acquire().await;
        let cooling = self.gate.cooldown_until.load(Ordering::Relaxed) > now_ms();
        let spacing = if cooling { self.gate.spacing_ms * 5 } else { self.gate.spacing_ms };
        {
            let last = *self.gate.last_start.lock().await;
            let wait = last + Duration::from_millis(spacing);
            let now = Instant::now();
            if wait > now {
                tokio::time::sleep(wait - now).await;
            }
        }
        if method == "eth_getLogs" {
            let extra = if cooling { self.gate.logs_spacing_ms * 3 } else { self.gate.logs_spacing_ms };
            let last = *self.gate.last_logs.lock().await;
            let wait = last + Duration::from_millis(extra);
            let now = Instant::now();
            if wait > now {
                tokio::time::sleep(wait - now).await;
            }
            *self.gate.last_logs.lock().await = Instant::now();
        }
        *self.gate.last_start.lock().await = Instant::now();
    }

    fn release(&self) {
        self.gate.sem.add_permits(1);
    }

    fn candidates(&self, method: &str) -> Vec<Arc<Endpoint>> {
        let now = now_ms();
        let able: Vec<_> = self
            .endpoints
            .iter()
            .filter(|e| method != "eth_getLogs" || e.cfg.logs)
            .cloned()
            .collect();
        let healthy: Vec<_> = able.iter().filter(|e| e.bad_until.load(Ordering::Relaxed) <= now).cloned().collect();
        if !healthy.is_empty() {
            return healthy;
        }
        able
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn hex_u64(v: &Value) -> anyhow::Result<u64> {
    let s = v.as_str().ok_or_else(|| anyhow::anyhow!("not hex"))?;
    Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?)
}

fn hex_u256(v: &Value) -> anyhow::Result<U256> {
    let s = v.as_str().ok_or_else(|| anyhow::anyhow!("not hex"))?;
    Ok(U256::from_str_radix(s.trim_start_matches("0x"), 16)?)
}

fn filter_to_json(f: &Filter) -> Value {
    serde_json::to_value(f).unwrap_or(json!({}))
}

#[allow(dead_code)]
fn _sol<T: SolValue>(_: T) {}
