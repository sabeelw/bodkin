use super::launches::LaunchEvent;
use alloy::primitives::{Address, B256};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryCoverage {
    pub chain_id: u64,
    pub factory: Address,
    pub from_block: u64,
    pub to_block: u64,
    pub anchor: B256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryStatus {
    Loading,
    Ready(HistoryCoverage),
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryCheckpoint {
    pub schema: u32,
    pub coverage: HistoryCoverage,
    launches: Vec<HistoryLaunch>,
    graduations: Vec<(Address, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HistoryLaunch {
    deployer: Address,
    token: Address,
    block: u64,
}

/// Who launched what, kept in memory. Built once from a chunked getLogs, then fed by the live stream.
/// Default window is two days at ~100 ms blocks (~1.73M blocks), not the old 11-hour 400k window.
pub struct DeployerIndex {
    launches: HashMap<Address, Vec<(u64, Address)>>,
    token_deployer: HashMap<Address, Address>,
    graduated_tokens: HashMap<Address, u64>,
    last_grad_block: u64,
    status: HistoryStatus,
    pub window_blocks: u64,
}

impl Default for DeployerIndex {
    fn default() -> Self {
        Self::new(1_728_000)
    }
}

impl DeployerIndex {
    pub fn new(window_blocks: u64) -> Self {
        Self {
            launches: HashMap::new(),
            token_deployer: HashMap::new(),
            graduated_tokens: HashMap::new(),
            last_grad_block: 0,
            status: HistoryStatus::Loading,
            window_blocks,
        }
    }

    pub fn note(&mut self, ev: &LaunchEvent) {
        if self.token_deployer.contains_key(&ev.token) {
            return;
        }
        self.token_deployer.insert(ev.token, ev.deployer);
        self.launches
            .entry(ev.deployer)
            .or_default()
            .push((ev.block_number, ev.token));
    }

    pub fn mark_graduated(&mut self, token: Address, block: u64) {
        if block == 0 {
            return;
        }
        self.graduated_tokens
            .entry(token)
            .and_modify(|known| *known = (*known).min(block))
            .or_insert(block);
        self.last_grad_block = self.last_grad_block.max(block);
    }

    pub fn set_last_grad_block(&mut self, b: u64) {
        self.last_grad_block = b;
    }

    pub fn last_grad_block(&self) -> u64 {
        self.last_grad_block
    }

    /// Launches by this deployer before `before_block` inside the window, and how many of them graduated.
    pub fn quick(&self, deployer: Address, before_block: u64) -> Option<(u32, u32)> {
        if !self.is_ready() {
            return None;
        }
        let floor = before_block.saturating_sub(self.window_blocks);
        let mut prior = 0usize;
        let mut graduated = 0usize;
        for (_, token) in self
            .launches
            .get(&deployer)
            .into_iter()
            .flatten()
            .filter(|(block, _)| *block < before_block && *block >= floor)
        {
            prior = prior.saturating_add(1);
            if self
                .graduated_tokens
                .get(token)
                .is_some_and(|block| *block < before_block)
            {
                graduated = graduated.saturating_add(1);
            }
        }
        Some((
            u32::try_from(prior).unwrap_or(u32::MAX),
            u32::try_from(graduated).unwrap_or(u32::MAX),
        ))
    }

    pub fn size(&self) -> (usize, usize, usize) {
        (
            self.launches.len(),
            self.token_deployer.len(),
            self.graduated_tokens.len(),
        )
    }

    pub fn history_status(&self) -> HistoryStatus {
        self.status.clone()
    }

    pub fn begin_history(&mut self) {
        self.status = HistoryStatus::Loading;
    }

    pub fn mark_ready(&mut self, coverage: HistoryCoverage) {
        self.status = HistoryStatus::Ready(coverage);
    }

    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.status = HistoryStatus::Failed(error.into());
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.status, HistoryStatus::Ready(_))
    }

    pub fn checkpoint(&self) -> Option<HistoryCheckpoint> {
        let HistoryStatus::Ready(coverage) = &self.status else {
            return None;
        };
        let mut launches = self
            .launches
            .iter()
            .flat_map(|(deployer, launches)| {
                launches.iter().map(|(block, token)| HistoryLaunch {
                    deployer: *deployer,
                    token: *token,
                    block: *block,
                })
            })
            .collect::<Vec<_>>();
        launches.sort_by_key(|launch| (launch.block, launch.token));
        let mut graduations = self
            .graduated_tokens
            .iter()
            .map(|(token, block)| (*token, *block))
            .collect::<Vec<_>>();
        graduations.sort_by_key(|(token, block)| (*block, *token));
        Some(HistoryCheckpoint {
            schema: 1,
            coverage: coverage.clone(),
            launches,
            graduations,
        })
    }

    pub fn restore(&mut self, checkpoint: HistoryCheckpoint) -> anyhow::Result<()> {
        anyhow::ensure!(
            checkpoint.schema == 1,
            "unsupported deployer checkpoint schema"
        );
        for launch in checkpoint.launches {
            if self.token_deployer.contains_key(&launch.token) {
                continue;
            }
            self.token_deployer.insert(launch.token, launch.deployer);
            self.launches
                .entry(launch.deployer)
                .or_default()
                .push((launch.block, launch.token));
        }
        for launches in self.launches.values_mut() {
            launches.sort_by_key(|(block, token)| (*block, *token));
        }
        for (token, block) in checkpoint.graduations {
            self.mark_graduated(token, block);
        }
        self.mark_ready(checkpoint.coverage);
        Ok(())
    }
}

pub struct Limiter {
    sem: Arc<Semaphore>,
}

impl Limiter {
    pub fn new(n: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(n)),
        }
    }

    pub async fn run<T, F, Fut>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _p = self.sem.acquire().await.expect("limiter closed");
        f().await
    }

    pub async fn run_timed<T, F, Fut>(&self, f: F) -> (T, u64, u64)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let queued = std::time::Instant::now();
        let _permit = self.sem.acquire().await.expect("limiter closed");
        let waited_ms = u64::try_from(queued.elapsed().as_millis()).unwrap_or(u64::MAX);
        let started = std::time::Instant::now();
        let value = f().await;
        let work_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        (value, waited_ms, work_ms)
    }
}

/// Shared index used by hunt / snipe / board.
pub type SharedIndex = Arc<Mutex<DeployerIndex>>;

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;

    fn launch(token: u8, deployer: u8, block_number: u64) -> LaunchEvent {
        LaunchEvent {
            token: Address::from([token; 20]),
            curve: Address::from([token.saturating_add(1); 20]),
            deployer: Address::from([deployer; 20]),
            pair_token: Address::ZERO,
            launch_config_id: U256::ZERO,
            graduation_threshold: U256::ZERO,
            block_number,
            tx_hash: B256::from([token; 32]),
            log_index: 0,
            detected_at_ms: 0,
            source: "test",
        }
    }

    fn coverage() -> HistoryCoverage {
        HistoryCoverage {
            chain_id: crate::chain::CHAIN_ID,
            factory: crate::chain::ADDR.pons_factory,
            from_block: 1,
            to_block: 100,
            anchor: B256::from([9; 32]),
        }
    }

    #[test]
    fn history_status_is_fail_closed() {
        let mut index = DeployerIndex::new(100);
        let deployer = Address::from([1; 20]);
        index.note(&launch(2, 1, 10));
        assert_eq!(index.quick(deployer, 20), None);
        index.mark_failed("partial scan");
        assert_eq!(index.quick(deployer, 20), None);
        index.mark_ready(coverage());
        assert_eq!(index.quick(deployer, 20), Some((1, 0)));
    }

    #[test]
    fn counts_window_edges_without_duplicate_or_future_leakage() {
        let mut index = DeployerIndex::new(10);
        let deployer = Address::from([1; 20]);
        let old = launch(2, 1, 9);
        let floor = launch(3, 1, 10);
        let recent = launch(4, 1, 19);
        let current = launch(5, 1, 20);
        for event in [&old, &floor, &recent, &current, &recent] {
            index.note(event);
        }
        index.mark_graduated(floor.token, 15);
        index.mark_graduated(recent.token, 21);
        index.mark_ready(coverage());
        assert_eq!(index.quick(deployer, 20), Some((2, 1)));
        assert_eq!(index.size(), (1, 4, 2));
    }

    #[test]
    fn graduation_keeps_earliest_nonzero_block() {
        let mut index = DeployerIndex::new(10);
        let token = Address::from([2; 20]);
        index.mark_graduated(token, 0);
        index.mark_graduated(token, 20);
        index.mark_graduated(token, 30);
        index.mark_graduated(token, 10);
        assert_eq!(index.graduated_tokens[&token], 10);
        assert_eq!(index.last_grad_block(), 30);
    }

    #[test]
    fn checkpoint_round_trip_preserves_verified_counts() {
        let mut index = DeployerIndex::new(100);
        let deployer = Address::from([1; 20]);
        let event = launch(2, 1, 10);
        index.note(&event);
        index.mark_graduated(event.token, 15);
        index.mark_ready(coverage());
        let encoded = serde_json::to_vec(&index.checkpoint().unwrap()).unwrap();
        let checkpoint: HistoryCheckpoint = serde_json::from_slice(&encoded).unwrap();
        let mut restored = DeployerIndex::new(100);
        restored.restore(checkpoint).unwrap();
        assert_eq!(restored.quick(deployer, 20), Some((1, 1)));
        assert_eq!(restored.history_status(), index.history_status());
    }

    #[tokio::test]
    async fn run_timed_reports_queue_and_work() {
        let limiter = Arc::new(Limiter::new(1));
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let held = tokio::spawn({
            let limiter = limiter.clone();
            async move {
                limiter
                    .run(|| async move {
                        let _ = acquired_tx.send(());
                        let _ = release_rx.await;
                    })
                    .await;
            }
        });
        acquired_rx.await.unwrap();
        let timed = tokio::spawn({
            let limiter = limiter.clone();
            async move {
                limiter
                    .run_timed(|| async {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        7
                    })
                    .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        release_tx.send(()).unwrap();
        let (value, waited_ms, work_ms) = timed.await.unwrap();
        held.await.unwrap();
        assert_eq!(value, 7);
        assert!(waited_ms >= 15, "waited_ms={waited_ms}");
        assert!(work_ms >= 15, "work_ms={work_ms}");
    }
}
