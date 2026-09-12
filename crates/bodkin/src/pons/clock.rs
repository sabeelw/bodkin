use crate::rpc::{Lane, Rpc};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// newHeads clock: running offset (arrival − header.timestamp) and cached base fee.
pub struct ChainClock {
    offset_ms: AtomicI64,
    base_fee: AtomicU64,
    last_ts: AtomicU64,
    last_block: AtomicU64,
}

impl Default for ChainClock {
    fn default() -> Self {
        Self {
            offset_ms: AtomicI64::new(0),
            base_fee: AtomicU64::new(100_000_000),
            last_ts: AtomicU64::new(0),
            last_block: AtomicU64::new(0),
        }
    }
}

impl ChainClock {
    pub fn note_header(&self, header_ts: u64, arrival_ms: u64, base_fee: u64, block: u64) {
        let header_ms = header_ts.saturating_mul(1000);
        let offset = arrival_ms as i64 - header_ms as i64;
        // EMA so a late header does not yank the whole clock.
        let prev = self.offset_ms.load(Ordering::Relaxed);
        let next = if prev == 0 { offset } else { (prev * 7 + offset) / 8 };
        self.offset_ms.store(next, Ordering::Relaxed);
        self.base_fee.store(base_fee, Ordering::Relaxed);
        self.last_ts.store(header_ts, Ordering::Relaxed);
        self.last_block.store(block, Ordering::Relaxed);
    }

    pub fn offset_ms(&self) -> i64 {
        self.offset_ms.load(Ordering::Relaxed)
    }

    pub fn base_fee(&self) -> u64 {
        self.base_fee.load(Ordering::Relaxed)
    }

    pub fn last_ts(&self) -> u64 {
        self.last_ts.load(Ordering::Relaxed)
    }

    pub fn last_block(&self) -> u64 {
        self.last_block.load(Ordering::Relaxed)
    }

    /// Sequencer-now: wall clock minus the running offset, in Unix seconds.
    pub fn sequencer_now(&self) -> u64 {
        let wall = now_ms() as i64;
        let adj = wall - self.offset_ms();
        (adj.max(0) as u64) / 1000
    }

    pub fn sleep_until_unix(&self, unix: u64) -> impl std::future::Future<Output = ()> {
        let offset = self.offset_ms();
        async move {
            let target_ms = unix.saturating_mul(1000);
            let wall_target = (target_ms as i64 + offset).max(0) as u64;
            let now = now_ms();
            if wall_target > now {
                tokio::time::sleep(std::time::Duration::from_millis(wall_target - now)).await;
            }
        }
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub async fn spawn_clock(rpc: Arc<Rpc>, clock: Arc<ChainClock>, ws_url: Option<String>) {
    tokio::spawn(async move {
        if let Some(url) = ws_url {
            if let Err(e) = pump_ws(&rpc, &clock, &url).await {
                tracing::warn!("clock ws: {e}");
            }
        }
        loop {
            match rpc.latest_header(Lane::Hot).await {
                Ok(h) => clock.note_header(h.timestamp, now_ms(), h.base_fee, h.number),
                Err(e) => tracing::debug!("clock poll: {e}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    });
}

async fn pump_ws(rpc: &Rpc, clock: &ChainClock, url: &str) -> anyhow::Result<()> {
    let mut s = rpc.subscribe_heads(url).await?;
    while let Some(h) = futures::StreamExt::next(&mut s).await {
        if let Ok(h) = h {
            clock.note_header(h.timestamp, now_ms(), h.base_fee, h.number);
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct HeaderView {
    pub number: u64,
    pub timestamp: u64,
    pub base_fee: u64,
}
