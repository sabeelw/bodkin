use crate::abi::{factory, topics};
use crate::chain::ADDR;
use crate::config::Config;
use crate::fmt::first_line;
use crate::pons::clock::now_ms;
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use futures_util::StreamExt;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct LaunchEvent {
    pub token: Address,
    pub curve: Address,
    pub deployer: Address,
    pub pair_token: Address,
    pub launch_config_id: U256,
    pub graduation_threshold: U256,
    pub block_number: u64,
    pub tx_hash: B256,
    pub log_index: u64,
    pub detected_at_ms: u64,
    pub source: &'static str,
}

#[derive(Debug, Clone)]
pub struct FeedHealth {
    pub mode: &'static str,
    pub last_launch_at: u64,
    pub last_ws_at: u64,
    pub recoveries: u64,
    pub note: String,
}

impl Default for FeedHealth {
    fn default() -> Self {
        Self {
            mode: "polling",
            last_launch_at: 0,
            last_ws_at: now_ms(),
            recoveries: 0,
            note: String::new(),
        }
    }
}

pub struct LaunchWatch {
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl LaunchWatch {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub async fn shutdown(mut self) {
        self.stop();
        for task in self.tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for LaunchWatch {
    fn drop(&mut self) {
        self.stop();
    }
}

fn to_event(log: &Log) -> Option<LaunchEvent> {
    let decoded = factory::TokenLaunched::decode_log(&log.clone().into()).ok()?;
    Some(LaunchEvent {
        token: decoded.token,
        curve: decoded.curve,
        deployer: decoded.deployer,
        pair_token: decoded.pairToken,
        launch_config_id: decoded.launchConfigId,
        graduation_threshold: decoded.graduationThreshold,
        block_number: log.block_number?,
        tx_hash: log.transaction_hash?,
        log_index: log.log_index?,
        detected_at_ms: 0,
        source: "unknown",
    })
}

const QUIET_MS: u64 = 45_000;

/// Raced WS TokenLaunched + HTTP poll watchdog. Dedup (tx_hash, log_index).
/// Watchdog first poll walks the quiet window (last seen block → head), not just `head`.
/// The caller supplies the shared `health` cell so the engine can expose real
/// feed state to the board.
pub fn watch_launches(
    rpc: Arc<Rpc>,
    cfg: Config,
    tx: mpsc::Sender<LaunchEvent>,
    health: Arc<parking_lot::Mutex<FeedHealth>>,
) -> LaunchWatch {
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    {
        let mut h = health.lock();
        h.mode = if cfg.rpc_ws.is_empty() {
            "polling"
        } else {
            "websocket"
        };
        h.last_ws_at = now_ms();
    }
    // Highest block an event was dispatched at — the "last seen" cursor.
    let last_block = Arc::new(AtomicU64::new(0));
    // Highest block the HTTP poller has scanned, events or not — advancing it
    // is what guarantees the poll loop never re-scans the same empty stretch.
    let scan_to = Arc::new(AtomicU64::new(0));
    let seen = Arc::new(parking_lot::Mutex::new(LruCache::<(B256, u64), ()>::new(
        NonZeroUsize::new(20_000).expect("nonzero launch dedup capacity"),
    )));

    let dispatch = {
        let seen = seen.clone();
        let health = health.clone();
        let tx = tx.clone();
        let last_block = last_block.clone();
        move |mut ev: LaunchEvent, source: &'static str, removed: bool| {
            let seen = seen.clone();
            let health = health.clone();
            let tx = tx.clone();
            let last_block = last_block.clone();
            async move {
                let key = (ev.tx_hash, ev.log_index);
                let should_send = {
                    let mut seen = seen.lock();
                    if removed {
                        seen.pop(&key);
                        health.lock().note = format!(
                            "launch {:#x} removed from canonical block {}",
                            ev.token, ev.block_number
                        );
                        false
                    } else {
                        seen.put(key, ()).is_none()
                    }
                };
                if !should_send {
                    return;
                }
                ev.detected_at_ms = now_ms();
                ev.source = source;
                last_block.fetch_max(ev.block_number, Ordering::SeqCst);
                {
                    let mut current = health.lock();
                    current.last_launch_at = ev.detected_at_ms;
                    if source == "websocket" {
                        current.last_ws_at = ev.detected_at_ms;
                    }
                }
                let _ = tx.send(ev).await;
            }
        }
    };

    // HTTP poller — owns `scan_to`, advancing it on every successful poll so
    // coverage never leaves a hole behind it.
    {
        let rpc = rpc.clone();
        let stop = stop.clone();
        let health = health.clone();
        let scan_to = scan_to.clone();
        let dispatch = dispatch.clone();
        let poll_ms = cfg.poll_ms;
        tasks.push(tokio::spawn(async move {
            let mut backoff = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let delay = poll_ms.saturating_mul(1 + backoff);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let last = scan_to.load(Ordering::SeqCst);
                match poll_once(&rpc, last).await {
                    Ok((evs, to)) => {
                        scan_to.store(to.max(last), Ordering::SeqCst);
                        backoff = backoff.saturating_sub(1);
                        for ev in evs {
                            dispatch(ev, "polling", false).await;
                        }
                    }
                    Err(e) => {
                        backoff = (backoff + 2).min(6);
                        if backoff >= 4 {
                            health.lock().note =
                                format!("poll error: {}", first_line(&e.to_string()));
                        }
                    }
                }
            }
        }));
    }

    // Raced WS
    if !cfg.rpc_ws.is_empty() {
        let rpc_ws = rpc.clone();
        let stop_ws = stop.clone();
        let health_ws = health.clone();
        let dispatch_ws = dispatch.clone();
        let urls = cfg.rpc_ws.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                if stop_ws.load(Ordering::SeqCst) {
                    break;
                }
                match raced_subscribe(
                    &rpc_ws,
                    &urls,
                    stop_ws.clone(),
                    health_ws.clone(),
                    dispatch_ws.clone(),
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        health_ws.lock().note = format!("ws: {}", first_line(&e.to_string()));
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            }
        }));

        // Watchdog: if WS is quiet for QUIET_MS, bump recoveries and make sure poll covers the gap.
        let health_w = health.clone();
        let stop_w = stop.clone();
        let last_block_w = last_block.clone();
        let scan_to_w = scan_to.clone();
        let rpc_w = rpc.clone();
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                if stop_w.load(Ordering::SeqCst) {
                    break;
                }
                let quiet = now_ms().saturating_sub(health_w.lock().last_ws_at);
                if quiet > QUIET_MS {
                    health_w.lock().recoveries += 1;
                    health_w.lock().mode = "polling";
                    health_w.lock().note =
                        format!("websocket quiet for {} s, polling the gap", quiet / 1000);
                    // First recovery after silence: walk min(last seen, last
                    // scanned) → head so nothing between the cursors is skipped.
                    let cursor = last_block_w
                        .load(Ordering::SeqCst)
                        .min(scan_to_w.load(Ordering::SeqCst));
                    if let Ok((evs, to)) = poll_once(&rpc_w, cursor).await {
                        scan_to_w.fetch_max(to, Ordering::SeqCst);
                        for ev in evs {
                            dispatch(ev, "polling", false).await;
                        }
                    }
                } else if quiet < 10_000 {
                    let mut h = health_w.lock();
                    if h.mode != "websocket" {
                        h.mode = "websocket";
                        h.note = "websocket back".into();
                    }
                }
            }
        }));
    }

    LaunchWatch { stop, tasks }
}

/// Poll `last+1..head` (capped at 2 000 blocks). Returns the events plus the
/// highest block actually scanned, so the caller can advance its cursor even
/// when the range was empty.
async fn poll_once(rpc: &Rpc, last: u64) -> anyhow::Result<(Vec<LaunchEvent>, u64)> {
    let head = rpc.block_number(Lane::Hot).await?;
    let from = if last == 0 {
        head.saturating_sub(1)
    } else {
        last.saturating_add(1)
    };
    if head < from {
        return Ok((vec![], head));
    }
    let to = if head.saturating_sub(from) > 2_000 {
        from + 2_000
    } else {
        head
    };
    let logs = rpc
        .get_logs(
            Lane::Hot,
            Filter::new()
                .address(ADDR.pons_factory)
                .event_signature(topics::token_launched())
                .from_block(from)
                .to_block(to),
        )
        .await?;
    Ok((logs.iter().filter_map(to_event).collect(), to))
}

async fn raced_subscribe<F, Fut>(
    rpc: &Rpc,
    urls: &[String],
    stop: Arc<AtomicBool>,
    health: Arc<parking_lot::Mutex<FeedHealth>>,
    dispatch: F,
) -> anyhow::Result<()>
where
    F: Fn(LaunchEvent, &'static str, bool) -> Fut + Send + Clone + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let filter = Filter::new()
        .address(ADDR.pons_factory)
        .event_signature(topics::token_launched());
    let mut streams = Vec::new();
    for url in urls {
        match rpc.subscribe_logs(url, filter.clone()).await {
            Ok(s) => streams.push(s),
            Err(e) => tracing::warn!("ws subscribe {url}: {e}"),
        }
    }
    if streams.is_empty() {
        anyhow::bail!("no websocket subscribed");
    }
    health.lock().last_ws_at = now_ms();
    let mut fused = futures_util::stream::select_all(streams);
    while let Some(item) = fused.next().await {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        match item {
            Ok(log) => {
                health.lock().last_ws_at = now_ms();
                if let Some(ev) = to_event(&log) {
                    dispatch(ev, "websocket", log.removed).await;
                }
            }
            Err(e) => {
                health.lock().note = format!("subscription error: {}", first_line(&e.to_string()));
            }
        }
    }
    Ok(())
}

pub async fn recent_launches(
    rpc: &Rpc,
    blocks: u64,
    deployer: Option<Address>,
    token: Option<Address>,
) -> anyhow::Result<Vec<LaunchEvent>> {
    let head = rpc.block_number(Lane::Background).await?;
    launches_in_range(rpc, head.saturating_sub(blocks), head, deployer, token).await
}

pub async fn launches_in_range(
    rpc: &Rpc,
    from: u64,
    to: u64,
    deployer: Option<Address>,
    token: Option<Address>,
) -> anyhow::Result<Vec<LaunchEvent>> {
    anyhow::ensure!(from <= to, "launch history range is reversed");
    let step: u64 = if deployer.is_some() || token.is_some() {
        100_000
    } else {
        25_000
    };
    let mut out = Vec::new();
    let mut b = from;
    while b <= to {
        let end = b.saturating_add(step - 1).min(to);
        let mut filter = Filter::new()
            .address(ADDR.pons_factory)
            .event_signature(topics::token_launched())
            .from_block(b)
            .to_block(end);
        if let Some(t) = token {
            filter = filter.topic1(addr_word(t));
        }
        if let Some(d) = deployer {
            filter = filter.topic3(addr_word(d));
        }
        let logs = rpc.get_logs(Lane::Background, filter).await?;
        out.extend(logs.iter().filter_map(to_event));
        if end == to {
            break;
        }
        b = end + 1;
    }
    out.sort_by_key(|e| (e.block_number, e.log_index));
    Ok(out)
}

/// Chunked scan of `PoolGraduated` over the last `blocks` blocks.
/// Returns (token, block_number) pairs — feeds the deployer graduation index.
pub async fn recent_graduations(rpc: &Rpc, blocks: u64) -> anyhow::Result<Vec<(Address, u64)>> {
    let head = rpc.block_number(Lane::Background).await?;
    graduations_in_range(rpc, head.saturating_sub(blocks), head).await
}

pub async fn graduations_in_range(
    rpc: &Rpc,
    from: u64,
    to: u64,
) -> anyhow::Result<Vec<(Address, u64)>> {
    anyhow::ensure!(from <= to, "graduation history range is reversed");
    let step: u64 = 25_000;
    let mut out = Vec::new();
    let mut b = from;
    while b <= to {
        let end = b.saturating_add(step - 1).min(to);
        let filter = Filter::new()
            .address(ADDR.pons_factory)
            .event_signature(topics::pool_graduated())
            .from_block(b)
            .to_block(end);
        let logs = rpc.get_logs(Lane::Background, filter).await?;
        for l in &logs {
            if let Ok(g) = factory::PoolGraduated::decode_log(&l.clone().into()) {
                out.push((g.token, l.block_number.unwrap_or(0)));
            }
        }
        if end == to {
            break;
        }
        b = end + 1;
    }
    Ok(out)
}

pub async fn find_launch(rpc: &Rpc, token: Address) -> anyhow::Result<Option<LaunchEvent>> {
    let head = rpc.block_number(Lane::Background).await?;
    let windows = [500_000u64, 2_000_000, 8_000_000, 32_000_000];
    let mut to = head;
    for w in windows {
        let from = to.saturating_sub(w);
        let filter = Filter::new()
            .address(ADDR.pons_factory)
            .event_signature(topics::token_launched())
            .topic1(addr_word(token))
            .from_block(from)
            .to_block(to);
        let logs = rpc.get_logs(Lane::Background, filter).await?;
        if let Some(ev) = logs.iter().find_map(to_event) {
            return Ok(Some(ev));
        }
        if from == 0 {
            break;
        }
        to = from.saturating_sub(1);
    }
    Ok(None)
}

fn addr_word(a: Address) -> B256 {
    B256::left_padding_from(a.as_slice())
}
