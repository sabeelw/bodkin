use crate::rpc::{Lane, Rpc};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// newHeads clock: running offset (arrival − header.timestamp) for display, a
/// pinned wall↔chain second boundary for scheduling, and cached base fee.
pub struct ChainClock {
    state: parking_lot::Mutex<ClockState>,
}

struct ClockState {
    offset_ms: i64, // arrival − header_ms EMA (board/debug observability)
    base_fee: u64,
    last_ts: u64,
    last_block: u64,
    last_arrival_ms: u64,
    last_observed: Option<Instant>,
    jitter_ms: u64,        // cadence jitter of second boundaries
    boundary_ts: u64,      // newest Unix second observed via first header of that second
    boundary_wall_ms: u64, // local wall ms when that first header arrived
    boundary_observed: Option<Instant>,
    boundary_observations: u64,
}

impl Default for ChainClock {
    fn default() -> Self {
        Self {
            state: parking_lot::Mutex::new(ClockState {
                offset_ms: 0,
                base_fee: 100_000_000,
                last_ts: 0,
                last_block: 0,
                last_arrival_ms: 0,
                last_observed: None,
                jitter_ms: 0,
                boundary_ts: 0,
                boundary_wall_ms: 0,
                boundary_observed: None,
                boundary_observations: 0,
            }),
        }
    }
}

impl ChainClock {
    pub fn note_header(&self, header_ts: u64, arrival_ms: u64, base_fee: u64, block: u64) {
        self.note_header_at(header_ts, arrival_ms, base_fee, block, Instant::now());
    }

    fn note_header_at(
        &self,
        header_ts: u64,
        arrival_ms: u64,
        base_fee: u64,
        block: u64,
        observed_at: Instant,
    ) {
        let mut state = self.state.lock();
        if block < state.last_block {
            return;
        }
        let header_ms = header_ts.saturating_mul(1000);
        let offset = i64::try_from(arrival_ms).unwrap_or(i64::MAX)
            - i64::try_from(header_ms).unwrap_or(i64::MAX);
        // EMA so a late header does not yank the whole clock.
        let next = if state.offset_ms == 0 {
            offset
        } else {
            (state.offset_ms.saturating_mul(7).saturating_add(offset)) / 8
        };
        state.offset_ms = next;
        // The first header carrying a new second pins that boundary; later
        // same-second headers must not move it.
        if state.boundary_ts == 0 || header_ts > state.boundary_ts {
            if let Some(previous) = state.boundary_observed {
                let interval =
                    u64::try_from(observed_at.saturating_duration_since(previous).as_millis())
                        .unwrap_or(u64::MAX);
                let expected = header_ts
                    .saturating_sub(state.boundary_ts)
                    .saturating_mul(1_000);
                let deviation = interval.abs_diff(expected);
                state.jitter_ms = if state.jitter_ms == 0 {
                    deviation
                } else {
                    (state.jitter_ms.saturating_mul(7) + deviation) / 8
                };
            }
            state.boundary_ts = header_ts;
            state.boundary_wall_ms = arrival_ms;
            state.boundary_observed = Some(observed_at);
            state.boundary_observations = state.boundary_observations.saturating_add(1);
        }
        state.base_fee = base_fee;
        state.last_ts = header_ts;
        state.last_block = block;
        state.last_arrival_ms = arrival_ms;
        state.last_observed = Some(observed_at);
    }

    pub fn offset_ms(&self) -> i64 {
        self.state.lock().offset_ms
    }

    pub fn base_fee(&self) -> u64 {
        self.state.lock().base_fee
    }

    pub fn last_ts(&self) -> u64 {
        self.state.lock().last_ts
    }

    pub fn last_block(&self) -> u64 {
        self.state.lock().last_block
    }

    /// Scheduling anchors on the newest pinned second boundary, not the
    /// arrival-offset EMA (which bakes in each block's within-second position).
    /// `chain_now_ms` interpolates inside the current second;
    /// `wall_for_chain_ms` projects a chain-ms moment onto the same wall line —
    /// so dry and live agree. Both fall back to the offset before any boundary.
    pub fn chain_now_ms(&self) -> i64 {
        let state = self.state.lock();
        if let Some(boundary_observed) = state.boundary_observed {
            let elapsed =
                u64::try_from(boundary_observed.elapsed().as_millis()).unwrap_or(u64::MAX);
            let chain_ms = state
                .boundary_ts
                .saturating_mul(1_000)
                .saturating_add(elapsed);
            return i64::try_from(chain_ms).unwrap_or(i64::MAX);
        }
        let chain_ms = i128::from(now_ms()) - i128::from(state.offset_ms);
        chain_ms.clamp(0, i128::from(i64::MAX)) as i64
    }

    pub fn wall_for_chain_ms(&self, chain_ms: u64) -> u64 {
        let state = self.state.lock();
        if state.boundary_ts != 0 {
            let delta = i128::from(chain_ms) - i128::from(state.boundary_ts.saturating_mul(1_000));
            return (i128::from(state.boundary_wall_ms) + delta).clamp(0, i128::from(u64::MAX))
                as u64;
        }
        (i128::from(chain_ms) + i128::from(state.offset_ms)).clamp(0, i128::from(u64::MAX)) as u64
    }

    pub fn instant_for_chain_ms(&self, chain_ms: u64) -> anyhow::Result<Instant> {
        let state = self.state.lock();
        let anchor = state
            .boundary_observed
            .ok_or_else(|| anyhow::anyhow!("chain clock has no boundary anchor"))?;
        let boundary_chain_ms = state.boundary_ts.saturating_mul(1_000);
        if chain_ms >= boundary_chain_ms {
            anchor
                .checked_add(Duration::from_millis(chain_ms - boundary_chain_ms))
                .ok_or_else(|| anyhow::anyhow!("chain deadline exceeds monotonic clock range"))
        } else {
            anchor
                .checked_sub(Duration::from_millis(boundary_chain_ms - chain_ms))
                .ok_or_else(|| anyhow::anyhow!("chain deadline precedes monotonic clock range"))
        }
    }

    pub fn sleep_until_chain_ms(
        &self,
        chain_ms: u64,
    ) -> impl Future<Output = anyhow::Result<()>> + use<> {
        let target = self.instant_for_chain_ms(chain_ms);
        async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(target?)).await;
            Ok(())
        }
    }

    /// Sequencer-now: wall clock minus the running offset, in Unix seconds.
    pub fn sequencer_now(&self) -> u64 {
        u64::try_from(self.chain_now_ms()).unwrap_or(0) / 1000
    }

    /// Whether the chain clock has actually been seeded by a header yet.
    pub fn seeded(&self) -> bool {
        self.state.lock().last_ts > 0
    }

    pub fn age_ms(&self) -> u64 {
        self.state
            .lock()
            .last_observed
            .map(|observed| u64::try_from(observed.elapsed().as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX)
    }

    pub fn jitter_ms(&self) -> u64 {
        self.state.lock().jitter_ms
    }

    pub fn boundary_observations(&self) -> u64 {
        self.state.lock().boundary_observations
    }

    pub fn require_quality(&self, max_age_ms: u64, max_jitter_ms: u64) -> anyhow::Result<()> {
        let state = self.state.lock();
        anyhow::ensure!(state.last_ts > 0, "chain clock is unseeded");
        anyhow::ensure!(
            state.boundary_observations >= 2,
            "chain clock has not observed a second boundary transition"
        );
        let age_ms = state
            .last_observed
            .map(|observed| u64::try_from(observed.elapsed().as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX);
        anyhow::ensure!(
            age_ms <= max_age_ms,
            "chain clock header is {age_ms} ms old"
        );
        anyhow::ensure!(
            state.jitter_ms <= max_jitter_ms,
            "chain clock jitter {} ms exceeds {max_jitter_ms} ms",
            state.jitter_ms
        );
        Ok(())
    }

    pub fn sleep_until_unix(&self, unix: u64) -> impl Future<Output = ()> + use<> {
        let target = self.instant_for_chain_ms(unix.saturating_mul(1_000));
        async move {
            if let Ok(target) = target {
                tokio::time::sleep_until(tokio::time::Instant::from_std(target)).await;
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
        let observed = Instant::now();
        // A header ~5 s old arriving now: wall is ahead of chain → offset > 0.
        c.note_header_at((now - 5_000) / 1000, now, 0, 1, observed);
        let off = c.offset_ms();
        assert!(off >= 5_000, "offset {off} should be ≥ 5 s");
        assert!(c.seeded());
        // The first header of a new second pins a wall↔chain boundary:
        // chain_now is the interpolated position inside that second and
        // scheduling projects off the boundary, not the stale offset.
        let ts = now / 1000;
        c.note_header_at(ts, now, 0, 2, observed + Duration::from_secs(5));
        assert!((c.chain_now_ms() - now as i64).abs() < 1_000);
        assert_eq!(c.wall_for_chain_ms(ts * 1000 + 300), now + 300);
        // A second boundary 1 s later extends the same wall↔chain line.
        c.note_header_at(ts + 1, now + 1_000, 0, 3, observed + Duration::from_secs(6));
        assert_eq!(c.wall_for_chain_ms((ts + 3) * 1000), now + 3_000);
    }

    #[test]
    fn quality_requires_stable_boundary_transitions_and_ignores_old_headers() {
        let clock = ChainClock::default();
        let now = now_ms();
        let observed = Instant::now();
        clock.note_header_at(now / 1000, now, 1, 10, observed);
        assert!(clock.require_quality(2_000, 1_500).is_err());
        clock.note_header_at(
            now / 1000 + 1,
            now + 1_000,
            1,
            11,
            observed + Duration::from_secs(1),
        );
        assert!(clock.require_quality(2_000, 1_500).is_ok());
        clock.note_header_at(1, now, 1, 9, observed + Duration::from_secs(2));
        assert_eq!(clock.last_block(), 11);
        // Consecutive-second boundaries 1.5 s apart are ~500 ms of cadence jitter.
        clock.note_header_at(
            now / 1000 + 2,
            now + 2_500,
            1,
            12,
            observed + Duration::from_millis(2_500),
        );
        assert!(clock.jitter_ms() > 100);
        assert!(clock.require_quality(2_000, 100).is_err());
    }

    #[test]
    fn same_second_headers_do_not_move_the_boundary() {
        let clock = ChainClock::default();
        let now = now_ms();
        let observed = Instant::now();
        let ts = now / 1000;
        clock.note_header_at(ts, now, 1, 10, observed);
        for block in 11..15 {
            clock.note_header_at(
                ts,
                now + 100 * (block - 10),
                1,
                block,
                observed + Duration::from_millis(100 * (block - 10)),
            );
        }
        assert_eq!(clock.wall_for_chain_ms(ts * 1000), now);
        assert_eq!(clock.jitter_ms(), 0);
        assert_eq!(clock.boundary_observations(), 1);
    }

    #[test]
    fn wall_clock_jumps_do_not_change_monotonic_interpolation() {
        let clock = ChainClock::default();
        let observed = Instant::now();
        clock.note_header_at(100, 100_000, 1, 1, observed);
        clock.note_header_at(101, 50_000, 1, 2, observed + Duration::from_secs(1));
        assert_eq!(clock.boundary_observations(), 2);
        assert_eq!(clock.jitter_ms(), 0);
        assert_eq!(
            clock.instant_for_chain_ms(101_500).unwrap(),
            observed + Duration::from_millis(1_500)
        );
    }
}
