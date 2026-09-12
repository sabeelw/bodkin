use crate::rpc::{Lane, Rpc};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// newHeads clock: running offset (arrival − header.timestamp) and cached base fee.
pub struct ChainClock {
    offset_ms: AtomicI64,
    base_fee: AtomicU64,
    last_ts: AtomicU64,
    last_block: AtomicU64,
    last_arrival_ms: AtomicU64,
    jitter_ms: AtomicU64,
}

impl Default for ChainClock {
    fn default() -> Self {
        Self {
            offset_ms: AtomicI64::new(0),
            base_fee: AtomicU64::new(100_000_000),
            last_ts: AtomicU64::new(0),
            last_block: AtomicU64::new(0),
            last_arrival_ms: AtomicU64::new(0),
            jitter_ms: AtomicU64::new(0),
        }
    }
}

impl ChainClock {
    pub fn note_header(&self, header_ts: u64, arrival_ms: u64, base_fee: u64, block: u64) {
        if block < self.last_block.load(Ordering::Relaxed) {
            return;
        }
        let header_ms = header_ts.saturating_mul(1000);
        let offset = arrival_ms as i64 - header_ms as i64;
        // EMA so a late header does not yank the whole clock.
        let prev = self.offset_ms.load(Ordering::Relaxed);
        let next = if prev == 0 {
            offset
        } else {
            (prev * 7 + offset) / 8
        };
        let deviation = offset.abs_diff(next);
        let previous_jitter = self.jitter_ms.load(Ordering::Relaxed);
        let jitter = if previous_jitter == 0 {
            deviation
        } else {
            (previous_jitter.saturating_mul(7) + deviation) / 8
        };
        self.offset_ms.store(next, Ordering::Relaxed);
        self.jitter_ms.store(jitter, Ordering::Relaxed);
        self.base_fee.store(base_fee, Ordering::Relaxed);
        self.last_ts.store(header_ts, Ordering::Relaxed);
        self.last_block.store(block, Ordering::Relaxed);
        self.last_arrival_ms.store(arrival_ms, Ordering::Relaxed);
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

    /// Single convention: `offset_ms = wall − chain` (positive when local time
    /// runs ahead of header timestamps). Both scheduling paths convert through
    /// these two helpers — `chain_now_ms` = wall − offset, `wall_for_chain_ms`
    /// = chain + offset — so dry and live agree.
    pub fn chain_now_ms(&self) -> i64 {
        now_ms() as i64 - self.offset_ms()
    }

    pub fn wall_for_chain_ms(&self, chain_ms: u64) -> u64 {
        (chain_ms as i64 + self.offset_ms()).max(0) as u64
    }

    /// Sequencer-now: wall clock minus the running offset, in Unix seconds.
    pub fn sequencer_now(&self) -> u64 {
        (self.chain_now_ms().max(0) as u64) / 1000
    }

    /// Whether the chain clock has actually been seeded by a header yet.
    pub fn seeded(&self) -> bool {
        self.last_ts.load(Ordering::Relaxed) > 0
    }

    pub fn age_ms(&self) -> u64 {
        now_ms().saturating_sub(self.last_arrival_ms.load(Ordering::Relaxed))
    }

    pub fn jitter_ms(&self) -> u64 {
        self.jitter_ms.load(Ordering::Relaxed)
    }

    pub fn require_quality(&self, max_age_ms: u64, max_jitter_ms: u64) -> anyhow::Result<()> {
        anyhow::ensure!(self.seeded(), "chain clock is unseeded");
        anyhow::ensure!(
            self.age_ms() <= max_age_ms,
            "chain clock header is {} ms old",
            self.age_ms()
        );
        anyhow::ensure!(
            self.jitter_ms() <= max_jitter_ms,
            "chain clock jitter {} ms exceeds {max_jitter_ms} ms",
            self.jitter_ms()
        );
        Ok(())
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub async fn spawn_clock(
    rpc: Arc<Rpc>,
    clock: Arc<ChainClock>,
    ws_url: Option<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Some(url) = &ws_url
                && let Err(error) = pump_ws(&rpc, &clock, url).await
            {
                tracing::debug!("clock ws: {error}");
            }
            match rpc.latest_header(Lane::Hot).await {
                Ok(header) => {
                    clock.note_header(header.timestamp, now_ms(), header.base_fee, header.number)
                }
                Err(error) => tracing::debug!("clock poll: {error}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    })
}

async fn pump_ws(rpc: &Rpc, clock: &ChainClock, url: &str) -> anyhow::Result<()> {
    let mut stream = rpc.subscribe_heads(url).await?;
    loop {
        let header = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            futures_util::StreamExt::next(&mut stream),
        )
        .await
        .map_err(|_| anyhow::anyhow!("newHeads silent for 2 seconds"))?
        .ok_or_else(|| anyhow::anyhow!("newHeads stream closed"))??;
        clock.note_header(header.timestamp, now_ms(), header.base_fee, header.number);
    }
}

#[derive(Debug, Clone)]
pub struct HeaderView {
    pub number: u64,
    pub hash: alloy::primitives::B256,
    pub timestamp: u64,
    pub base_fee: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_sign_is_wall_minus_chain() {
        let c = ChainClock::default();
        let now = now_ms();
        // A header ~5 s old arriving now: wall is ahead of chain → offset > 0.
        c.note_header((now - 5_000) / 1000, now, 0, 1);
        let off = c.offset_ms();
        assert!(off >= 5_000, "offset {off} should be ≥ 5 s");
        // Scheduling a chain-ms moment must push it out by the offset.
        let chain_ms = now + 10_000;
        assert_eq!(c.wall_for_chain_ms(chain_ms), chain_ms + off as u64);
        // chain_now = wall − offset, so it lands just before the wall clock.
        let chain_now = c.chain_now_ms();
        assert!(chain_now <= now as i64 && chain_now > now as i64 - 30_000);
        assert!(c.seeded());
    }

    #[test]
    fn quality_refuses_unstable_and_ignores_old_headers() {
        let clock = ChainClock::default();
        let now = now_ms();
        clock.note_header(now / 1000, now, 1, 10);
        assert!(clock.require_quality(2_000, 1_500).is_ok());
        clock.note_header(1, now, 1, 9);
        assert_eq!(clock.last_block(), 10);
        clock.note_header(now.saturating_sub(20_000) / 1000, now, 1, 11);
        assert!(clock.require_quality(2_000, 100).is_err());
    }
}
