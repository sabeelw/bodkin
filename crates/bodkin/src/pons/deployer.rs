use super::launches::LaunchEvent;
use alloy::primitives::Address;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Who launched what, kept in memory. Built once from a chunked getLogs, then fed by the live stream.
/// Default window is two days at ~100 ms blocks (~1.73M blocks), not the old 11-hour 400k window.
pub struct DeployerIndex {
    launches: HashMap<String, Vec<(u64, String)>>,
    token_deployer: HashMap<String, String>,
    graduated_tokens: HashSet<String>,
    last_grad_block: u64,
    pub ready: bool,
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
            graduated_tokens: HashSet::new(),
            last_grad_block: 0,
            ready: false,
            window_blocks,
        }
    }

    pub fn note(&mut self, ev: &LaunchEvent) {
        let d = format!("{:#x}", ev.deployer);
        let t = format!("{:#x}", ev.token);
        if self.token_deployer.contains_key(&t) {
            return;
        }
        self.token_deployer.insert(t.clone(), d.clone());
        self.launches
            .entry(d)
            .or_default()
            .push((ev.block_number, t));
    }

    pub fn mark_graduated(&mut self, token: Address) {
        self.graduated_tokens.insert(format!("{token:#x}"));
    }

    pub fn set_last_grad_block(&mut self, b: u64) {
        self.last_grad_block = b;
    }

    pub fn last_grad_block(&self) -> u64 {
        self.last_grad_block
    }

    /// Launches by this deployer before `before_block` inside the window, and how many of them graduated.
    pub fn quick(&self, deployer: Address, before_block: u64) -> Option<(u32, u32)> {
        if !self.ready {
            return None;
        }
        let d = format!("{deployer:#x}");
        let floor = before_block.saturating_sub(self.window_blocks);
        let list: Vec<_> = self
            .launches
            .get(&d)
            .into_iter()
            .flatten()
            .filter(|(b, _)| *b < before_block && *b >= floor)
            .collect();
        let prior = list.len() as u32;
        let graduated = list
            .iter()
            .filter(|(_, t)| self.graduated_tokens.contains(t))
            .count() as u32;
        Some((prior, graduated))
    }

    pub fn size(&self) -> (usize, usize, usize) {
        (
            self.launches.len(),
            self.token_deployer.len(),
            self.graduated_tokens.len(),
        )
    }

    pub fn mark_ready(&mut self) {
        self.ready = true;
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
