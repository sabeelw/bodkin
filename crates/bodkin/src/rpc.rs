use crate::chain::USER_AGENT;
use crate::config::{Config, EndpointCfg};
use crate::pons::clock::{HeaderView, now_ms};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolCall;
use backon::{ExponentialBuilder, Retryable};
use futures_util::{Stream, StreamExt};
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

const MAX_RPC_BODY_BYTES: u64 = 2 * 1024 * 1024;

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
    pub latency: LatencyStats,
    pub endpoints: Vec<EpStat>,
}

#[derive(Debug, Clone)]
pub struct EpStat {
    pub label: String,
    pub logs: bool,
    pub benched: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LatencyStats {
    pub count: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

pub struct LatencyRecorder {
    histogram: parking_lot::Mutex<Histogram<u64>>,
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self {
            histogram: parking_lot::Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000, 3)
                    .expect("valid latency histogram bounds"),
            ),
        }
    }
}

impl LatencyRecorder {
    pub fn record(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .clamp(1, 60_000_000);
        let _ = self.histogram.lock().record(micros);
    }

    pub fn stats(&self) -> LatencyStats {
        let histogram = self.histogram.lock();
        if histogram.is_empty() {
            return LatencyStats::default();
        }
        LatencyStats {
            count: histogram.len(),
            p50_us: histogram.value_at_quantile(0.50),
            p95_us: histogram.value_at_quantile(0.95),
            p99_us: histogram.value_at_quantile(0.99),
            max_us: histogram.max(),
        }
    }
}

struct Endpoint {
    cfg: EndpointCfg,
    bad_until: AtomicU64,
    client: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct RpcAttemptError {
    message: String,
    retryable: bool,
    retry_after: Option<Duration>,
}

impl RpcAttemptError {
    fn retryable(message: impl Into<String>, retry_after: Option<Duration>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            retry_after,
        }
    }

    fn terminal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            retry_after: None,
        }
    }
}

struct Gate {
    /// Permits any lane may hold. `shared + hot` always equals `in_flight`.
    shared: Semaphore,
    /// Reserved capacity only the hot lane may take, so enrichment traffic can
    /// never starve a time-critical read or a confirm poll.
    hot: Semaphore,
    /// Next reserved send slot. Held only long enough to bump the reservation,
    /// so concurrent callers get distinct, monotonically spaced start times.
    next_start: Mutex<Instant>,
    cooldown_until: AtomicU64,
    throttled: AtomicU64,
    /// Requests currently spacing/sleeping or waiting on a permit.
    waiting: AtomicU64,
    spacing_ms: u64,
    logs_spacing_ms: u64,
    in_flight: usize,
}

pub struct Rpc {
    endpoints: Vec<Arc<Endpoint>>,
    gate: Arc<Gate>,
    latency: LatencyRecorder,
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
            gate: {
                let in_flight = cfg.rpc_in_flight.max(1);
                // Keep at least one shared permit for non-hot lanes; a 0-permit
                // hot semaphore just makes Hot fall back to shared via select.
                let hot_reserve = if in_flight > 1 {
                    (in_flight / 3).max(1).min(in_flight - 1)
                } else {
                    0
                };
                Arc::new(Gate {
                    shared: Semaphore::new(in_flight - hot_reserve),
                    hot: Semaphore::new(hot_reserve),
                    next_start: Mutex::new(Instant::now() - Duration::from_secs(1)),
                    cooldown_until: AtomicU64::new(0),
                    throttled: AtomicU64::new(0),
                    waiting: AtomicU64::new(0),
                    spacing_ms: cfg.rpc_spacing_ms,
                    logs_spacing_ms: cfg.rpc_logs_spacing_ms,
                    in_flight,
                })
            },
            latency: LatencyRecorder::default(),
            next_id: AtomicU64::new(1),
        })
    }

    pub fn stats(&self) -> GateStats {
        let now = now_ms();
        let idle = self.gate.shared.available_permits() + self.gate.hot.available_permits();
        GateStats {
            active: (self.gate.in_flight as u64).saturating_sub(idle as u64),
            queued: self.gate.waiting.load(Ordering::Relaxed) as usize,
            in_flight: self.gate.in_flight,
            spacing_ms: self.gate.spacing_ms,
            logs_spacing_ms: self.gate.logs_spacing_ms,
            throttled: self.gate.throttled.load(Ordering::Relaxed),
            cooling_down: self.gate.cooldown_until.load(Ordering::Relaxed) > now,
            latency: self.latency.stats(),
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
        let started = Instant::now();
        let mut attempt = 0usize;
        let result = (|| {
            let current = attempt;
            attempt += 1;
            let params = params.clone();
            async move { self.call_once(lane, method, params, current).await }
        })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(100))
                .with_max_delay(Duration::from_secs(2))
                .with_total_delay(Some(Duration::from_secs(15)))
                .with_max_times(7)
                .with_jitter(),
        )
        .when(|error| error.retryable)
        .adjust(|error, default| error.retry_after.or(default))
        .await;
        self.latency.record(started.elapsed());
        result.map_err(|error| anyhow::anyhow!("rpc {method}: gave up ({error})"))
    }

    async fn call_once(
        &self,
        lane: Lane,
        method: &str,
        params: Value,
        attempt: usize,
    ) -> Result<Value, RpcAttemptError> {
        let list = self.candidates(method);
        if list.is_empty() {
            return Err(RpcAttemptError::terminal(format!(
                "no configured endpoint serves {method}"
            )));
        }
        let endpoint = list[attempt % list.len()].clone();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        let (status, retry_after, text) = {
            let _permit = self.acquire(lane, method).await;
            let response = endpoint
                .client
                .post(&endpoint.cfg.url)
                .header("content-type", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|error| {
                    endpoint
                        .bad_until
                        .store(now_ms() + 5_000, Ordering::Relaxed);
                    RpcAttemptError::retryable(format!("{}: {error}", endpoint.cfg.label), None)
                })?;
            let status = response.status().as_u16();
            let retry_after = parse_retry_after(response.headers());
            if response
                .content_length()
                .is_some_and(|length| length > MAX_RPC_BODY_BYTES)
            {
                return Err(RpcAttemptError::terminal(format!(
                    "{}: response body exceeds {} bytes",
                    endpoint.cfg.label, MAX_RPC_BODY_BYTES
                )));
            }
            let bytes = response.bytes().await.map_err(|error| {
                endpoint
                    .bad_until
                    .store(now_ms() + 5_000, Ordering::Relaxed);
                RpcAttemptError::retryable(
                    format!("{}: response body: {error}", endpoint.cfg.label),
                    None,
                )
            })?;
            if bytes.len() as u64 > MAX_RPC_BODY_BYTES {
                return Err(RpcAttemptError::terminal(format!(
                    "{}: response body exceeds {} bytes",
                    endpoint.cfg.label, MAX_RPC_BODY_BYTES
                )));
            }
            let text = String::from_utf8(bytes.to_vec()).map_err(|error| {
                RpcAttemptError::retryable(
                    format!(
                        "{}: response body is not UTF-8: {error}",
                        endpoint.cfg.label
                    ),
                    None,
                )
            })?;
            (status, retry_after, text)
        };
        if status == 429 || status == 503 {
            self.gate.throttled.fetch_add(1, Ordering::Relaxed);
            let delay = retry_after.unwrap_or(Duration::from_secs(3));
            let delay_ms = u64::try_from(delay.as_millis())
                .unwrap_or(u64::MAX)
                .min(60_000);
            self.gate
                .cooldown_until
                .store(now_ms().saturating_add(delay_ms), Ordering::Relaxed);
            endpoint.bad_until.store(
                now_ms().saturating_add(delay_ms.max(4_000)),
                Ordering::Relaxed,
            );
            return Err(RpcAttemptError::retryable(
                format!("{}: HTTP {status}", endpoint.cfg.label),
                Some(delay),
            ));
        }
        if status == 403 && text.to_ascii_lowercase().contains("cloudflare") {
            endpoint
                .bad_until
                .store(now_ms() + 60_000, Ordering::Relaxed);
            return Err(RpcAttemptError::retryable(
                format!("{}: bot-protection challenge", endpoint.cfg.label),
                Some(Duration::from_millis(10)),
            ));
        }
        if status >= 500 {
            endpoint
                .bad_until
                .store(now_ms() + 4_000, Ordering::Relaxed);
            return Err(RpcAttemptError::retryable(
                format!("{}: HTTP {status}", endpoint.cfg.label),
                retry_after,
            ));
        }
        if !(200..300).contains(&status) {
            return Err(RpcAttemptError::terminal(format!(
                "{}: HTTP {status}",
                endpoint.cfg.label
            )));
        }
        let value: Value = serde_json::from_str(&text).map_err(|_| {
            RpcAttemptError::retryable(
                format!("{}: HTTP {status} non-JSON", endpoint.cfg.label),
                None,
            )
        })?;
        if let Some(error) = value.get("error") {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| error.to_string());
            let lower = message.to_ascii_lowercase();
            if matches!(code, 429 | -32005)
                || lower.contains("rate limit")
                || lower.contains("too many requests")
            {
                self.gate.throttled.fetch_add(1, Ordering::Relaxed);
                return Err(RpcAttemptError::retryable(
                    format!("{}: {message}", endpoint.cfg.label),
                    retry_after,
                ));
            }
            return Err(RpcAttemptError::terminal(format!(
                "{}: {message}",
                endpoint.cfg.label
            )));
        }
        Ok(value.get("result").cloned().unwrap_or(Value::Null))
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
        hex_u256(
            &self
                .call(
                    lane,
                    "eth_getBalance",
                    json!([format!("{addr:#x}"), "latest"]),
                )
                .await?,
        )
    }

    pub async fn get_code(&self, lane: Lane, addr: Address) -> anyhow::Result<Bytes> {
        let value = self
            .call(lane, "eth_getCode", json!([format!("{addr:#x}"), "latest"]))
            .await?;
        hex_bytes(&value, "eth_getCode")
    }

    pub async fn get_transaction_count(
        &self,
        lane: Lane,
        addr: Address,
        pending: bool,
    ) -> anyhow::Result<u64> {
        let tag = if pending { "pending" } else { "latest" };
        hex_u64(
            &self
                .call(
                    lane,
                    "eth_getTransactionCount",
                    json!([format!("{addr:#x}"), tag]),
                )
                .await?,
        )
    }

    pub async fn eth_call<C: SolCall>(
        &self,
        lane: Lane,
        to: Address,
        call: C,
        from: Option<Address>,
    ) -> anyhow::Result<C::Return> {
        let data = format!("0x{}", hex::encode(call.abi_encode()));
        let mut obj = json!({"to": format!("{to:#x}"), "data": data});
        if let Some(f) = from {
            obj["from"] = json!(format!("{f:#x}"));
        }
        let v = self.call(lane, "eth_call", json!([obj, "latest"])).await?;
        let s = v
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("eth_call non-hex"))?;
        let bytes = hex::decode(s.trim_start_matches("0x"))?;
        C::abi_decode_returns(&bytes).map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn eth_call_raw(
        &self,
        lane: Lane,
        to: Address,
        data: &[u8],
    ) -> anyhow::Result<Bytes> {
        self.eth_call_raw_at(lane, to, data, json!("latest")).await
    }

    async fn eth_call_raw_at(
        &self,
        lane: Lane,
        to: Address,
        data: &[u8],
        block: Value,
    ) -> anyhow::Result<Bytes> {
        let obj = json!({"to": format!("{to:#x}"), "data": format!("0x{}", hex::encode(data))});
        let value = self.call(lane, "eth_call", json!([obj, block])).await?;
        hex_bytes(&value, "eth_call")
    }

    pub async fn multicall3(
        &self,
        lane: Lane,
        calls: &[(Address, Vec<u8>)],
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        self.multicall3_at(lane, calls, json!("latest")).await
    }

    pub(crate) async fn multicall3_at(
        &self,
        lane: Lane,
        calls: &[(Address, Vec<u8>)],
        block: Value,
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        use crate::abi::multicall3::{Call3, aggregate3Call};
        use crate::chain::MULTICALL3;
        let encoded = aggregate3Call {
            calls: calls
                .iter()
                .map(|(t, d)| Call3 {
                    target: *t,
                    allowFailure: true,
                    callData: Bytes::from(d.clone()),
                })
                .collect(),
        }
        .abi_encode();
        let raw = self
            .eth_call_raw_at(lane, MULTICALL3, &encoded, block)
            .await?;
        let decoded = aggregate3Call::abi_decode_returns(&raw)?;
        Ok(decoded
            .into_iter()
            .map(|r| {
                if r.success {
                    Some(r.returnData.to_vec())
                } else {
                    None
                }
            })
            .collect())
    }

    pub async fn get_logs(&self, lane: Lane, filter: Filter) -> anyhow::Result<Vec<Log>> {
        let v = self
            .call(lane, "eth_getLogs", json!([filter_to_json(&filter)]))
            .await?;
        Ok(serde_json::from_value(v)?)
    }

    pub async fn get_transaction(
        &self,
        lane: Lane,
        hash: B256,
    ) -> anyhow::Result<crate::pons::enrich::TxView> {
        let v = self
            .call(
                lane,
                "eth_getTransactionByHash",
                json!([format!("{hash:#x}")]),
            )
            .await?;
        parse_tx_view(&v)
    }

    pub async fn get_transaction_receipt(
        &self,
        lane: Lane,
        hash: B256,
    ) -> anyhow::Result<crate::pons::enrich::ReceiptView> {
        let v = self
            .call(
                lane,
                "eth_getTransactionReceipt",
                json!([format!("{hash:#x}")]),
            )
            .await?;
        parse_receipt_view(&v)
    }

    pub async fn latest_header(&self, lane: Lane) -> anyhow::Result<HeaderView> {
        let v = self
            .call(lane, "eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        parse_header_view(&v)
    }

    pub async fn block_timestamp(&self, lane: Lane, number: u64) -> anyhow::Result<u64> {
        let v = self
            .call(
                lane,
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )
            .await?;
        hex_u64(v.get("timestamp").unwrap_or(&Value::Null))
    }

    pub async fn block_hash(&self, lane: Lane, number: u64) -> anyhow::Result<B256> {
        let value = self
            .call(
                lane,
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )
            .await?;
        anyhow::ensure!(!value.is_null(), "block {number} not found");
        anyhow::ensure!(
            hex_u64(value.get("number").unwrap_or(&Value::Null))? == number,
            "RPC returned the wrong block number for {number}"
        );
        value
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("block {number} hash missing"))?
            .parse()
            .map_err(|_| anyhow::anyhow!("block {number} hash malformed"))
    }

    pub async fn subscribe_logs(
        &self,
        ws_url: &str,
        filter: Filter,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Log>> + Send> {
        use jsonrpsee::core::client::SubscriptionClientT;
        let client = jsonrpsee::ws_client::WsClientBuilder::default()
            .max_response_size(MAX_RPC_BODY_BYTES as u32)
            .max_buffer_capacity_per_subscription(1_024)
            .request_timeout(Duration::from_secs(10))
            .connection_timeout(Duration::from_secs(10))
            .set_tcp_no_delay(true)
            .build(ws_url)
            .await?;
        let subscription = client
            .subscribe::<Log, _>(
                "eth_subscribe",
                jsonrpsee::rpc_params!["logs", filter_to_json(&filter)],
                "eth_unsubscribe",
            )
            .await?;
        Ok(subscription.map(move |result| {
            let _client = &client;
            result.map_err(Into::into)
        }))
    }

    pub async fn subscribe_heads(
        &self,
        ws_url: &str,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<HeaderView>> + Send> {
        use jsonrpsee::core::client::SubscriptionClientT;
        let client = jsonrpsee::ws_client::WsClientBuilder::default()
            .max_response_size(MAX_RPC_BODY_BYTES as u32)
            .max_buffer_capacity_per_subscription(64)
            .request_timeout(Duration::from_secs(10))
            .connection_timeout(Duration::from_secs(10))
            .set_tcp_no_delay(true)
            .build(ws_url)
            .await?;
        let subscription = client
            .subscribe::<Value, _>(
                "eth_subscribe",
                jsonrpsee::rpc_params!["newHeads"],
                "eth_unsubscribe",
            )
            .await?;
        Ok(subscription.map(move |result| {
            let _client = &client;
            parse_header_view(&result?)
        }))
    }

    /// Reserve a spaced send slot, wait for it, then take a capacity permit.
    /// The returned guard holds the permit until the caller drops it after the
    /// response body has been read.
    async fn acquire(&self, lane: Lane, method: &str) -> SemaphorePermit<'_> {
        self.gate.waiting.fetch_add(1, Ordering::Relaxed);
        let _waiting = scopeguard::guard(&self.gate.waiting, |waiting| {
            waiting.fetch_sub(1, Ordering::Relaxed);
        });
        let cooling = self.gate.cooldown_until.load(Ordering::Relaxed) > now_ms();
        let mut spacing = self.gate.spacing_ms;
        if method == "eth_getLogs" {
            spacing = spacing.max(self.gate.logs_spacing_ms);
        }
        // Hot lane keeps tighter spacing; everyone backs off harder on cooldown.
        let spacing = match lane {
            Lane::Hot => spacing / 5,
            _ => spacing,
        } * if cooling { 3 } else { 1 };
        let start_at = {
            let mut next = self.gate.next_start.lock().await;
            let now = Instant::now();
            let start = (*next).max(now);
            *next = start + Duration::from_millis(spacing.max(1));
            start
        };
        let now = Instant::now();
        if start_at > now {
            tokio::time::sleep(start_at - now).await;
        }
        match lane {
            Lane::Hot => match self.gate.hot.try_acquire() {
                Ok(p) => p,
                Err(_) => tokio::select! {
                    p = self.gate.shared.acquire() => p,
                    p = self.gate.hot.acquire() => p,
                }
                .expect("gate semaphore closed"),
            },
            _ => self
                .gate
                .shared
                .acquire()
                .await
                .expect("gate semaphore closed"),
        }
    }

    fn candidates(&self, method: &str) -> Vec<Arc<Endpoint>> {
        let now = now_ms();
        let able: Vec<_> = self
            .endpoints
            .iter()
            .filter(|e| method != "eth_getLogs" || e.cfg.logs)
            .cloned()
            .collect();
        let healthy: Vec<_> = able
            .iter()
            .filter(|e| e.bad_until.load(Ordering::Relaxed) <= now)
            .cloned()
            .collect();
        if !healthy.is_empty() {
            return healthy;
        }
        able
    }
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds.min(10)));
    }
    let deadline = chrono::DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&chrono::Utc);
    let millis = (deadline - chrono::Utc::now())
        .num_milliseconds()
        .clamp(0, 10_000) as u64;
    Some(Duration::from_millis(millis))
}

fn hex_bytes(value: &Value, field: &str) -> anyhow::Result<Bytes> {
    let encoded = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("{field} result is not a string"))?;
    let hex = encoded
        .strip_prefix("0x")
        .ok_or_else(|| anyhow::anyhow!("{field} result has no 0x prefix"))?;
    Ok(Bytes::from(hex::decode(hex).map_err(|error| {
        anyhow::anyhow!("{field} result is not hex: {error}")
    })?))
}

fn hex_u64(v: &Value) -> anyhow::Result<u64> {
    let s = v.as_str().ok_or_else(|| anyhow::anyhow!("not hex"))?;
    Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?)
}

fn hex_u256(v: &Value) -> anyhow::Result<U256> {
    let s = v.as_str().ok_or_else(|| anyhow::anyhow!("not hex"))?;
    Ok(U256::from_str_radix(s.trim_start_matches("0x"), 16)?)
}

fn opt_addr(v: &Value, field: &str) -> anyhow::Result<Option<Address>> {
    match v.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => {
            let s = x
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("tx.{field} not a string"))?;
            s.parse()
                .map(Some)
                .map_err(|_| anyhow::anyhow!("tx.{field} malformed"))
        }
    }
}

/// Strict `eth_getTransactionByHash` result. Null or missing/malformed
/// sender, value, or calldata refuses instead of becoming an empty transaction.
pub fn parse_tx_view(v: &Value) -> anyhow::Result<crate::pons::enrich::TxView> {
    if v.is_null() {
        anyhow::bail!("transaction not found (null result)");
    }
    let from_s = v
        .get("from")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("tx.from missing"))?;
    let from: Address = from_s
        .parse()
        .map_err(|_| anyhow::anyhow!("tx.from malformed"))?;
    let to = opt_addr(v, "to")?;
    let value = hex_u256(
        v.get("value")
            .ok_or_else(|| anyhow::anyhow!("tx.value missing"))?,
    )?;
    let input = hex_bytes(
        v.get("input")
            .or_else(|| v.get("data"))
            .ok_or_else(|| anyhow::anyhow!("tx.input missing"))?,
        "tx.input",
    )?;
    Ok(crate::pons::enrich::TxView {
        from,
        to,
        value,
        input,
    })
}

fn parse_header_view(value: &Value) -> anyhow::Result<HeaderView> {
    Ok(HeaderView {
        number: hex_u64(value.get("number").unwrap_or(&Value::Null))?,
        hash: value
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("header hash missing"))?
            .parse()
            .map_err(|_| anyhow::anyhow!("header hash malformed"))?,
        timestamp: hex_u64(value.get("timestamp").unwrap_or(&Value::Null))?,
        base_fee: value
            .get("baseFeePerGas")
            .map(hex_u64)
            .transpose()?
            .unwrap_or(0),
    })
}

/// Strict `eth_getTransactionReceipt` result. `null` means pending/unknown and
/// is an error so callers can keep polling or refuse explicitly; a present
/// `status` of `0x0` is a real revert signal, not a missing field.
pub fn parse_receipt_view(v: &Value) -> anyhow::Result<crate::pons::enrich::ReceiptView> {
    use crate::pons::enrich::ReceiptView;
    if v.is_null() {
        anyhow::bail!("receipt not found (null result)");
    }
    let logs: Vec<Log> = serde_json::from_value(
        v.get("logs")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("receipt.logs missing"))?,
    )?;
    let status = match v.get("status") {
        None | Some(Value::Null) => None,
        Some(x) => match hex_u64(x)? {
            0 => Some(false),
            1 => Some(true),
            n => anyhow::bail!("receipt.status is neither 0 nor 1: {n}"),
        },
    };
    let block_number = match v.get("blockNumber") {
        None | Some(Value::Null) => None,
        Some(x) => Some(hex_u64(x)?),
    };
    let block_hash = match v.get("blockHash").and_then(Value::as_str) {
        Some(value) => Some(
            value
                .parse::<B256>()
                .map_err(|_| anyhow::anyhow!("receipt.blockHash malformed"))?,
        ),
        None => None,
    };
    let block_timestamp = match v.get("blockTimestamp") {
        None | Some(Value::Null) => None,
        Some(x) => Some(hex_u64(x)?),
    };
    let gas_used = match v.get("gasUsed") {
        None | Some(Value::Null) => None,
        Some(x) => Some(hex_u256(x)?),
    };
    let effective_gas_price = match v.get("effectiveGasPrice") {
        None | Some(Value::Null) => None,
        Some(x) => Some(hex_u256(x)?),
    };
    let tx_hash = match v.get("transactionHash").and_then(|x| x.as_str()) {
        Some(s) => Some(
            s.parse::<B256>()
                .map_err(|_| anyhow::anyhow!("receipt.transactionHash malformed"))?,
        ),
        None => None,
    };
    let contract_address = opt_addr(v, "contractAddress")?;
    if status.is_some()
        && (block_number.is_none()
            || block_hash.is_none()
            || tx_hash.is_none()
            || gas_used.is_none()
            || effective_gas_price.is_none())
    {
        anyhow::bail!(
            "mined receipt is missing blockNumber, blockHash, transactionHash, gasUsed, or effectiveGasPrice"
        );
    }
    Ok(ReceiptView {
        logs,
        block_timestamp,
        status,
        block_number,
        block_hash,
        tx_hash,
        contract_address,
        gas_used,
        effective_gas_price,
    })
}

fn filter_to_json(f: &Filter) -> Value {
    serde_json::to_value(f).unwrap_or(json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, Bytes as BodyBytes};
    use axum::extract::State;
    use axum::http::{HeaderValue, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use axum::{Json, Router};
    use std::convert::Infallible;
    use std::sync::atomic::AtomicUsize;

    fn test_cfg(in_flight: usize, spacing_ms: u64) -> Config {
        Config {
            rpc_http: vec![crate::config::EndpointCfg {
                url: "http://127.0.0.1:1".into(),
                logs: true,
                label: "test".into(),
            }],
            rpc_ws: vec![],
            sequencer_url: "http://127.0.0.1:1".into(),
            helper: None,
            poll_ms: 300,
            rpc_in_flight: in_flight,
            rpc_spacing_ms: spacing_ms,
            rpc_logs_spacing_ms: 0,
            board_port: 0,
        }
    }

    #[test]
    fn latency_recorder_reports_tail_percentiles() {
        let recorder = LatencyRecorder::default();
        for micros in [10, 20, 30, 1_000] {
            recorder.record(Duration::from_micros(micros));
        }
        let stats = recorder.stats();
        assert_eq!(stats.count, 4);
        assert!(stats.p50_us >= 20);
        assert!(stats.p99_us >= 1_000);
        assert!(stats.max_us >= stats.p99_us);
    }

    #[test]
    fn tx_view_null_and_garbage_refuse() {
        assert!(parse_tx_view(&Value::Null).is_err());
        let bad = json!({"from": "0xzz", "to": "0x0000000000000000000000000000000000000001"});
        assert!(parse_tx_view(&bad).is_err());
        let missing_from = json!({"to": "0x0000000000000000000000000000000000000001"});
        assert!(parse_tx_view(&missing_from).is_err());
        let valid = json!({
            "from":"0x0000000000000000000000000000000000000001",
            "to":"0x0000000000000000000000000000000000000002",
            "value":"0x0","input":"0x1234"
        });
        assert_eq!(
            parse_tx_view(&valid).unwrap().input,
            Bytes::from_static(&[0x12, 0x34])
        );
        let mut missing_value = valid.clone();
        missing_value.as_object_mut().unwrap().remove("value");
        assert!(parse_tx_view(&missing_value).is_err());
        let mut missing_input = valid;
        missing_input.as_object_mut().unwrap().remove("input");
        assert!(parse_tx_view(&missing_input).is_err());
        assert!(hex_bytes(&json!("1234"), "test").is_err());
        assert!(hex_bytes(&json!("0xzz"), "test").is_err());
    }

    #[test]
    fn header_requires_canonical_identity() {
        let header = parse_header_view(&json!({
            "number":"0x1",
            "hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "timestamp":"0x2",
            "baseFeePerGas":"0x3"
        }))
        .unwrap();
        assert_eq!(header.number, 1);
        assert_eq!(header.timestamp, 2);
        assert!(parse_header_view(&json!({"number":"0x1","timestamp":"0x2"})).is_err());
    }

    #[test]
    fn receipt_view_status_is_truthful() {
        assert!(parse_receipt_view(&Value::Null).is_err());
        let ok = parse_receipt_view(&json!({
            "status": "0x1",
            "blockNumber": "0x10",
            "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x1",
            "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "logs": []
        }))
        .unwrap();
        assert_eq!(ok.status, Some(true));
        assert_eq!(ok.block_number, Some(16));
        assert_eq!(ok.gas_used, Some(U256::from(21_000u64)));
        let bad = parse_receipt_view(&json!({
            "status": "0x0",
            "blockNumber": "0x11",
            "blockHash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x1",
            "transactionHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
            "logs": []
        }))
        .unwrap();
        assert_eq!(bad.status, Some(false));
        let nostatus = parse_receipt_view(&json!({"logs": []})).unwrap();
        assert_eq!(nostatus.status, None);
        assert!(parse_receipt_view(&json!({"status":"0x2","logs":[]})).is_err());
        assert!(parse_receipt_view(&json!({"status":"0x1","logs":[]})).is_err());
        assert!(parse_receipt_view(&json!({"status":"0x1","blockNumber":"0x1","transactionHash":"0x2222222222222222222222222222222222222222222222222222222222222222"})).is_err());
    }

    async fn retry_handler(
        State(calls): State<Arc<AtomicUsize>>,
        Json(request): Json<Value>,
    ) -> Response {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        if call < 2 {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(reqwest::header::RETRY_AFTER, HeaderValue::from_static("0"))],
                "busy",
            )
                .into_response();
        }
        Json(json!({"jsonrpc":"2.0","id":request["id"],"result":"0x2a"})).into_response()
    }

    #[tokio::test]
    async fn backon_retries_retryable_statuses_and_honors_retry_after() {
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = calls.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(retry_handler))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let mut config = test_cfg(1, 0);
        config.rpc_http[0].url = format!("http://{address}");
        let rpc = Rpc::new(&config).unwrap();
        assert_eq!(
            rpc.call(Lane::Hot, "eth_blockNumber", json!([]))
                .await
                .unwrap(),
            json!("0x2a")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    async fn terminal_handler(
        State(calls): State<Arc<AtomicUsize>>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        calls.fetch_add(1, Ordering::SeqCst);
        Json(
            json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32602,"message":"invalid params"}}),
        )
    }

    #[tokio::test]
    async fn backon_does_not_retry_terminal_json_rpc_errors() {
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = calls.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(terminal_handler))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let mut config = test_cfg(1, 0);
        config.rpc_http[0].url = format!("http://{address}");
        let rpc = Rpc::new(&config).unwrap();
        assert!(rpc.call(Lane::Hot, "eth_call", json!([])).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[test]
    fn retry_after_delta_seconds_is_parsed() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, HeaderValue::from_static("12"));
        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(10)));
    }

    async fn delayed_body_handler(State(calls): State<Arc<AtomicUsize>>) -> Response {
        calls.fetch_add(1, Ordering::SeqCst);
        let body = Body::from_stream(futures_util::stream::once(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, Infallible>(BodyBytes::from_static(
                br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#,
            ))
        }));
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(body)
            .unwrap()
    }

    #[tokio::test]
    async fn gate_permit_is_held_until_the_response_body_is_consumed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = calls.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(delayed_body_handler))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let mut config = test_cfg(1, 0);
        config.rpc_http[0].url = format!("http://{address}");
        let rpc = Arc::new(Rpc::new(&config).unwrap());
        let first_rpc = rpc.clone();
        let first = tokio::spawn(async move {
            first_rpc
                .call(Lane::Hot, "eth_blockNumber", json!([]))
                .await
        });
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let second_rpc = rpc.clone();
        let second = tokio::spawn(async move {
            second_rpc
                .call(Lane::Hot, "eth_blockNumber", json!([]))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(first.await.unwrap().is_ok());
        assert!(second.await.unwrap().is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn gate_capacity_is_bounded() {
        let rpc = Rpc::new(&test_cfg(1, 0)).unwrap();
        let p1 = rpc.acquire(Lane::Hot, "eth_blockNumber").await;
        // With in_flight = 1 the second permit cannot be granted until p1 drops.
        let waited = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            rpc.acquire(Lane::Hot, "eth_blockNumber"),
        )
        .await;
        assert!(
            waited.is_err(),
            "second permit should have queued behind in_flight=1"
        );
        drop(p1);
        let p2 = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rpc.acquire(Lane::Hot, "eth_blockNumber"),
        )
        .await;
        assert!(p2.is_ok(), "permit must be released on drop");
    }

    #[tokio::test]
    async fn cancelled_gate_wait_does_not_leak_queue_depth() {
        let rpc = Rpc::new(&test_cfg(1, 0)).unwrap();
        let permit = rpc.acquire(Lane::Background, "x").await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                rpc.acquire(Lane::Background, "x")
            )
            .await
            .is_err()
        );
        assert_eq!(rpc.stats().queued, 0);
        drop(permit);
    }

    #[tokio::test]
    async fn hot_reserve_keeps_shared_capacity() {
        // in_flight = 3 → hot_reserve = 1, shared = 2. Two Background permits
        // must still be acquirable while a Hot permit is held.
        let rpc = Rpc::new(&test_cfg(3, 0)).unwrap();
        let hot = rpc.acquire(Lane::Hot, "x").await;
        let b1 = rpc.acquire(Lane::Background, "x").await;
        let b2 = rpc.acquire(Lane::Background, "x").await;
        // Third background must wait — shared capacity is in_flight − reserve.
        let waited = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            rpc.acquire(Lane::Background, "x"),
        )
        .await;
        assert!(waited.is_err());
        drop(b1);
        drop(b2);
        drop(hot);
    }
}
