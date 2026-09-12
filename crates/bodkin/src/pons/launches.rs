use crate::abi::{factory, topics};
use crate::chain::ADDR;
use crate::config::Config;
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use futures::{future::FutureExt, StreamExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
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
    pub seen_at_ms: u64,
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
        Self { mode: "polling", last_launch_at: 0, last_ws_at: now_ms(), recoveries: 0, note: String::new() }
    }
}

pub struct LaunchWatch {
    stop: Arc<AtomicBool>,
    health: Arc<parking_lot::Mutex<FeedHealth>>,
}

impl LaunchWatch {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }
    pub fn health(&self) -> FeedHealth {
        self.health.lock().clone()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
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
        seen_at_ms: now_ms(),
    })
}

const QUIET_MS: u64 = 45_000;

/// Raced WS TokenLaunched + HTTP poll watchdog. Dedup (tx_hash, log_index).
/// Watchdog first poll walks the quiet window (last seen block → head), not just `head`.
pub fn watch_launches(rpc: Arc<Rpc>, cfg: Config, tx: mpsc::UnboundedSender<LaunchEvent>) -> LaunchWatch {
    let stop = Arc::new(AtomicBool::new(false));
    let health = Arc::new(parking_lot::Mutex::new(FeedHealth {
        mode: if cfg.rpc_ws.is_empty() { "polling" } else { "websocket" },
        last_launch_at: 0,
        last_ws_at: now_ms(),
        recoveries: 0,
        note: String::new(),
    }));
    let last_block = Arc::new(AtomicU64::new(0));
    let seen = Arc::new(parking_lot::Mutex::new(HashSet::<(B256, u64)>::new()));

    let dispatch = {
        let seen = seen.clone();
        let health = health.clone();
        let tx = tx.clone();
        let last_block = last_block.clone();
        move |ev: LaunchEvent, source: &'static str| {
            let mut s = seen.lock();
            let key = (ev.tx_hash, ev.log_index);
            if !s.insert(key) {
                return;
            }
            if s.len() > 20_000 {
                s.clear();
                s.insert(key);
            }
            drop(s);
            last_block.store(ev.block_number, Ordering::SeqCst);
            let mut h = health.lock();
            h.last_launch_at = now_ms();
            if source == "websocket" {
                h.last_ws_at = now_ms();
            }
            drop(h);
            let _ = tx.send(ev);
        }
    };

    // HTTP poller
    {
        let rpc = rpc.clone();
        let stop = stop.clone();
        let health = health.clone();
        let last_block = last_block.clone();
        let dispatch = dispatch.clone();
        let poll_ms = cfg.poll_ms;
        tokio::spawn(async move {
            let mut backoff = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let delay = poll_ms.saturating_mul(1 + backoff);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                match poll_once(&rpc, last_block.load(Ordering::SeqCst)).await {
                    Ok(evs) => {
                        backoff = backoff.saturating_sub(1);
                        for ev in evs {
                            dispatch(ev, "polling");
                        }
                    }
                    Err(e) => {
                        backoff = (backoff + 2).min(6);
                        if backoff >= 4 {
                            health.lock().note = format!("poll error: {}", first_line(&e.to_string()));
                        }
                    }
                }
            }
        });
    }

    // Raced WS
    if !cfg.rpc_ws.is_empty() {
        let rpc_ws = rpc.clone();
        let stop_ws = stop.clone();
        let health_ws = health.clone();
        let dispatch_ws = dispatch.clone();
        let urls = cfg.rpc_ws.clone();
        tokio::spawn(async move {
            loop {
                if stop_ws.load(Ordering::SeqCst) {
                    break;
                }
                match raced_subscribe(&rpc_ws, &urls, stop_ws.clone(), health_ws.clone(), dispatch_ws.clone()).await {
                    Ok(()) => {}
                    Err(e) => {
                        health_ws.lock().note = format!("ws: {}", first_line(&e.to_string()));
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            }
        });

        // Watchdog: if WS is quiet for QUIET_MS, bump recoveries and make sure poll covers the gap.
        let health_w = health.clone();
        let stop_w = stop.clone();
        let last_block_w = last_block.clone();
        let rpc_w = rpc.clone();
        tokio::spawn(async move {
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
                    health_w.lock().note = format!("websocket quiet for {} s, polling the gap", quiet / 1000);
                    // First recovery after silence: walk last_seen → head, not just `head`.
                    if let Ok(evs) = poll_once(&rpc_w, last_block_w.load(Ordering::SeqCst)).await {
                        for ev in evs {
                            dispatch(ev, "polling");
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
        });
    }

    LaunchWatch { stop, health }
}

async fn poll_once(rpc: &Rpc, last: u64) -> anyhow::Result<Vec<LaunchEvent>> {
    let head = rpc.block_number(Lane::Hot).await?;
    let from = if last == 0 { head.saturating_sub(1) } else { last.saturating_add(1) };
    if head < from {
        return Ok(vec![]);
    }
    let to = if head.saturating_sub(from) > 2_000 { from + 2_000 } else { head };
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
    Ok(logs.iter().filter_map(to_event).collect())
}

async fn raced_subscribe(
    rpc: &Rpc,
    urls: &[String],
    stop: Arc<AtomicBool>,
    health: Arc<parking_lot::Mutex<FeedHealth>>,
    dispatch: impl Fn(LaunchEvent, &'static str) + Send + Clone + 'static,
) -> anyhow::Result<()> {
    let filter = Filter::new().address(ADDR.pons_factory).event_signature(topics::token_launched());
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
    let mut fused = futures::stream::select_all(streams);
    while let Some(item) = fused.next().await {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        match item {
            Ok(log) => {
                health.lock().last_ws_at = now_ms();
                if let Some(ev) = to_event(&log) {
                    dispatch(ev, "websocket");
                }
            }
            Err(e) => {
                health.lock().note = format!("subscription error: {}", first_line(&e.to_string()));
            }
        }
    }
    Ok(())
}

pub async fn recent_launches(rpc: &Rpc, blocks: u64, deployer: Option<Address>, token: Option<Address>) -> anyhow::Result<Vec<LaunchEvent>> {
    let head = rpc.block_number(Lane::Background).await?;
    let from = head.saturating_sub(blocks);
    let step: u64 = if deployer.is_some() || token.is_some() { 100_000 } else { 25_000 };
    let mut out = Vec::new();
    let mut b = from;
    while b <= head {
        let to = (b + step - 1).min(head);
        let mut filter = Filter::new()
            .address(ADDR.pons_factory)
            .event_signature(topics::token_launched())
            .from_block(b)
            .to_block(to);
        if let Some(t) = token {
            filter = filter.topic1(addr_word(t));
        }
        if let Some(d) = deployer {
            filter = filter.topic3(addr_word(d));
        }
        let logs = rpc.get_logs(Lane::Background, filter).await?;
        out.extend(logs.iter().filter_map(to_event));
        if to == head {
            break;
        }
        b = to + 1;
    }
    out.sort_by_key(|e| (e.block_number, e.log_index));
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

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or(s).chars().take(160).collect()
}

// keep FutureExt imported for race helpers used by feed
#[allow(dead_code)]
fn _fuse<F: std::future::Future>(f: F) -> impl std::future::Future {
    f.fuse()
}
