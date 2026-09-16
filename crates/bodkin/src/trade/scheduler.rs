use crate::pons::clock::ChainClock;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};
use tokio::sync::Notify;
use tokio::time::Instant;

pub const ROUTINE_EXIT_MAX_DEFERRAL: Duration = Duration::from_millis(100);
pub const DISPATCH_WAKE_TOLERANCE_MS: u64 = 25;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationClass {
    Emergency,
    DeadlineEntry,
    RoutineExit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryDeadline {
    pub boundary_chain_ms: u64,
    pub dispatch_chain_ms: u64,
    pub latest_dispatch_chain_ms: u64,
}

impl EntryDeadline {
    pub fn new(launched_at: u64, entry_second: u64, lead_ms: u64) -> anyhow::Result<Self> {
        let boundary_chain_ms = launched_at
            .checked_add(entry_second)
            .and_then(|second| second.checked_mul(1_000))
            .ok_or_else(|| anyhow::anyhow!("entry boundary overflows milliseconds"))?;
        let dispatch_chain_ms = boundary_chain_ms.saturating_sub(lead_ms);
        let latest_dispatch_chain_ms = dispatch_chain_ms
            .checked_add(DISPATCH_WAKE_TOLERANCE_MS.min(lead_ms))
            .ok_or_else(|| anyhow::anyhow!("entry dispatch tolerance overflows milliseconds"))?;
        Ok(Self {
            boundary_chain_ms,
            dispatch_chain_ms,
            latest_dispatch_chain_ms,
        })
    }

    pub fn preparation_open(self, chain_now_ms: i64) -> bool {
        u64::try_from(chain_now_ms).is_ok_and(|now| now <= self.dispatch_chain_ms)
    }

    pub fn dispatch_open(self, chain_now_ms: i64) -> bool {
        u64::try_from(chain_now_ms).is_ok_and(|now| now <= self.latest_dispatch_chain_ms)
    }

    pub fn dispatch_instant(self, clock: &ChainClock) -> anyhow::Result<StdInstant> {
        clock.instant_for_chain_ms(self.dispatch_chain_ms)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AcquireError {
    #[error("execution deadline expired")]
    DeadlineExpired,
}

#[derive(Clone)]
pub struct ExecutionScheduler {
    inner: Arc<Inner>,
}

impl Default for ExecutionScheduler {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }
}

impl ExecutionScheduler {
    pub async fn acquire(
        &self,
        class: OperationClass,
        deadline: Option<StdInstant>,
    ) -> Result<ExecutionPermit, AcquireError> {
        let deadline = deadline.map(Instant::from_std);
        let now = Instant::now();
        if deadline.is_some_and(|value| value <= now) {
            return Err(AcquireError::DeadlineExpired);
        }
        let id = {
            let mut state = self.inner.state.lock();
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.waiters.push(Waiter {
                id,
                class,
                enqueued: now,
                deadline,
            });
            id
        };
        let mut registration = WaitRegistration {
            inner: self.inner.clone(),
            id,
            enqueued: now,
            active: true,
        };
        loop {
            let notified = self.inner.notify.notified();
            let now = Instant::now();
            let acquired = {
                let mut state = self.inner.state.lock();
                if deadline.is_some_and(|value| value <= now) {
                    remove_waiter(&mut state, id);
                    registration.active = false;
                    None
                } else if !state.owned && next_waiter(&state.waiters, now) == Some(id) {
                    remove_waiter(&mut state, id);
                    state.owned = true;
                    registration.active = false;
                    Some(ExecutionPermit {
                        inner: self.inner.clone(),
                        waited: now.saturating_duration_since(registration.enqueued),
                    })
                } else {
                    None
                }
            };
            match acquired {
                Some(permit) => return Ok(permit),
                None if deadline.is_some_and(|value| value <= now) => {
                    self.inner.notify.notify_waiters();
                    return Err(AcquireError::DeadlineExpired);
                }
                None => {}
            }
            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            } else {
                notified.await;
            }
        }
    }
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    notify: Notify,
}

#[derive(Default)]
struct State {
    owned: bool,
    next_id: u64,
    waiters: Vec<Waiter>,
}

struct Waiter {
    id: u64,
    class: OperationClass,
    enqueued: Instant,
    deadline: Option<Instant>,
}

fn next_waiter(waiters: &[Waiter], now: Instant) -> Option<u64> {
    waiters
        .iter()
        .filter(|waiter| waiter.deadline.is_none_or(|deadline| deadline > now))
        .min_by_key(|waiter| {
            let rank = match waiter.class {
                OperationClass::Emergency => 0,
                OperationClass::RoutineExit
                    if now.saturating_duration_since(waiter.enqueued)
                        >= ROUTINE_EXIT_MAX_DEFERRAL =>
                {
                    1
                }
                OperationClass::DeadlineEntry => 2,
                OperationClass::RoutineExit => 3,
            };
            (rank, waiter.id)
        })
        .map(|waiter| waiter.id)
}

fn remove_waiter(state: &mut State, id: u64) {
    if let Some(index) = state.waiters.iter().position(|waiter| waiter.id == id) {
        state.waiters.swap_remove(index);
    }
}

struct WaitRegistration {
    inner: Arc<Inner>,
    id: u64,
    enqueued: Instant,
    active: bool,
}

impl Drop for WaitRegistration {
    fn drop(&mut self) {
        if self.active {
            remove_waiter(&mut self.inner.state.lock(), self.id);
            self.inner.notify.notify_waiters();
        }
    }
}

pub struct ExecutionPermit {
    inner: Arc<Inner>,
    waited: Duration,
}

impl ExecutionPermit {
    pub fn waited(&self) -> Duration {
        self.waited
    }
}

impl Drop for ExecutionPermit {
    fn drop(&mut self) {
        self.inner.state.lock().owned = false;
        self.inner.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    async fn record_acquisition(
        scheduler: ExecutionScheduler,
        class: OperationClass,
        tx: mpsc::UnboundedSender<OperationClass>,
    ) {
        let _permit = scheduler.acquire(class, None).await.unwrap();
        tx.send(class).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn emergency_acquires_ahead_of_entry_and_routine() {
        let scheduler = ExecutionScheduler::default();
        let owner = scheduler
            .acquire(OperationClass::DeadlineEntry, None)
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let routine = tokio::spawn(record_acquisition(
            scheduler.clone(),
            OperationClass::RoutineExit,
            tx.clone(),
        ));
        tokio::task::yield_now().await;
        let entry = tokio::spawn(record_acquisition(
            scheduler.clone(),
            OperationClass::DeadlineEntry,
            tx.clone(),
        ));
        tokio::task::yield_now().await;
        let emergency = tokio::spawn(record_acquisition(scheduler, OperationClass::Emergency, tx));
        tokio::task::yield_now().await;
        drop(owner);
        assert_eq!(rx.recv().await, Some(OperationClass::Emergency));
        assert_eq!(rx.recv().await, Some(OperationClass::DeadlineEntry));
        assert_eq!(rx.recv().await, Some(OperationClass::RoutineExit));
        routine.await.unwrap();
        entry.await.unwrap();
        emergency.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn entry_acquires_ahead_of_fresh_routine() {
        let scheduler = ExecutionScheduler::default();
        let owner = scheduler
            .acquire(OperationClass::Emergency, None)
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let routine = tokio::spawn(record_acquisition(
            scheduler.clone(),
            OperationClass::RoutineExit,
            tx.clone(),
        ));
        tokio::task::yield_now().await;
        let entry = tokio::spawn(record_acquisition(
            scheduler,
            OperationClass::DeadlineEntry,
            tx,
        ));
        tokio::task::yield_now().await;
        drop(owner);
        assert_eq!(rx.recv().await, Some(OperationClass::DeadlineEntry));
        assert_eq!(rx.recv().await, Some(OperationClass::RoutineExit));
        routine.await.unwrap();
        entry.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn aged_routine_acquires_ahead_of_entry() {
        let scheduler = ExecutionScheduler::default();
        let owner = scheduler
            .acquire(OperationClass::Emergency, None)
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let routine = tokio::spawn(record_acquisition(
            scheduler.clone(),
            OperationClass::RoutineExit,
            tx.clone(),
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(ROUTINE_EXIT_MAX_DEFERRAL).await;
        let entry = tokio::spawn(record_acquisition(
            scheduler,
            OperationClass::DeadlineEntry,
            tx,
        ));
        tokio::task::yield_now().await;
        drop(owner);
        assert_eq!(rx.recv().await, Some(OperationClass::RoutineExit));
        assert_eq!(rx.recv().await, Some(OperationClass::DeadlineEntry));
        routine.await.unwrap();
        entry.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn expired_entry_never_acquires() {
        let scheduler = ExecutionScheduler::default();
        let result = scheduler
            .acquire(
                OperationClass::DeadlineEntry,
                StdInstant::now().checked_sub(Duration::from_secs(1)),
            )
            .await;
        assert!(matches!(result, Err(AcquireError::DeadlineExpired)));
        assert!(
            scheduler
                .acquire(OperationClass::RoutineExit, None)
                .await
                .is_ok()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_waiter_does_not_block_next_waiter() {
        let scheduler = ExecutionScheduler::default();
        let owner = scheduler
            .acquire(OperationClass::Emergency, None)
            .await
            .unwrap();
        let cancelled = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .acquire(OperationClass::Emergency, None)
                    .await
                    .unwrap()
            }
        });
        tokio::task::yield_now().await;
        cancelled.abort();
        assert!(matches!(cancelled.await, Err(error) if error.is_cancelled()));
        let next = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .acquire(OperationClass::RoutineExit, None)
                    .await
                    .unwrap()
            }
        });
        tokio::task::yield_now().await;
        drop(owner);
        assert!(next.await.is_ok());
    }

    #[test]
    fn entry_deadline_has_a_strict_preparation_cutoff_and_wake_tolerance() {
        let deadline = EntryDeadline::new(10, 2, 150).unwrap();
        assert_eq!(deadline.boundary_chain_ms, 12_000);
        assert_eq!(deadline.dispatch_chain_ms, 11_850);
        assert_eq!(deadline.latest_dispatch_chain_ms, 11_875);
        assert!(deadline.preparation_open(11_850));
        assert!(!deadline.preparation_open(11_851));
        assert!(deadline.dispatch_open(11_875));
        assert!(!deadline.dispatch_open(11_876));
        assert!(!deadline.dispatch_open(-1));
        assert_eq!(
            EntryDeadline::new(10, 2, 10)
                .unwrap()
                .latest_dispatch_chain_ms,
            12_000
        );
        assert!(EntryDeadline::new(u64::MAX, 1, 0).is_err());
    }
}
