use crate::Config;
use crate::engine::{SnipeRules, decide, live_gate, pick_exit};
use crate::outcomes::{OutcomeKind, OutcomeLog};
use crate::pons::clock::{ChainClock, now_ms, spawn_clock};
use crate::pons::deployer::{Limiter, SharedIndex};
use crate::pons::enrich::{dev_share_pct, enrich_launch, has_socials, is_contract};
use crate::pons::fingerprint::FarmDetector;
use crate::pons::launches::{FeedHealth, LaunchEvent, find_launch, watch_launches};
use crate::pons::stream::FlowTracker;
use crate::pons::tax::{boundary_instant, snipe_tax_bps};
use crate::rpc::{Lane, Rpc};
use crate::score::{ScoreContext, score_launch};
use crate::style::{info, muted, neon, on_neon, warn};
use crate::trade::burst::{
    BurstClass, BurstCtl, SignedTx, clock_fire, encode_buy_once, presign_burst, shadow_result,
};
use crate::trade::curve::{parse_curve_buy_tokens, parse_curve_refund};
use crate::trade::exec::{LiveExec, TxFinal, confirm};
use crate::trade::journal::{JournalEvent, OperationSpec, TxJournal};
use crate::trade::pool::{sell_anywhere, value_now};
use crate::trade::positions::{Position, PositionStore, pnl_pct};
use crate::trade::state::StateDb;
use crate::trade::submitter::{SendOutcome, Submitter};
use crate::trade::wallet::Wallet;
use alloy::primitives::{Address, U256};
use alloy::sol_types::SolEvent;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::mpsc;

pub type Emit = Arc<dyn Fn(serde_json::Value) + Send + Sync>;

fn halt(stop: &AtomicBool, paused: &AtomicBool) {
    paused.store(true, Ordering::SeqCst);
    stop.store(true, Ordering::SeqCst);
}

pub struct EngineHandle {
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    spent: Arc<Mutex<U256>>,
    feed: Arc<Mutex<FeedHealth>>,
    clock: Arc<ChainClock>,
    pub rules: Arc<Mutex<SnipeRules>>,
    close_tx: mpsc::Sender<String>,
    positions: Arc<PositionStore>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl EngineHandle {
    pub fn stop(&self) {
        self.paused.store(true, Ordering::SeqCst);
        self.stop.store(true, Ordering::SeqCst);
    }
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }
    pub fn resume(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.stop.load(Ordering::SeqCst), "engine stopped");
        self.paused.store(false, Ordering::SeqCst);
        Ok(())
    }
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }
    pub fn spent(&self) -> U256 {
        *self.spent.lock()
    }
    pub fn feed(&self) -> FeedHealth {
        self.feed.lock().clone()
    }
    pub fn clock(&self) -> &ChainClock {
        &self.clock
    }
    pub fn positions(&self) -> Vec<Position> {
        self.positions.load()
    }
    pub fn request_close(&self, id: String) -> anyhow::Result<()> {
        self.close_tx
            .try_send(id)
            .map_err(|e| anyhow::anyhow!("engine close queue unavailable: {e}"))
    }
    pub async fn shutdown(&self) {
        self.stop();
        if let Some(task) = self.task.lock().await.take()
            && let Err(e) = task.await
        {
            tracing::warn!("engine task: {e}");
        }
    }

    #[cfg(test)]
    pub(crate) fn test_handle(positions: Arc<PositionStore>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = stop.clone();
        let (close_tx, mut close_rx) = mpsc::channel::<String>(32);
        let task = tokio::spawn(async move {
            while !task_stop.load(Ordering::SeqCst) {
                tokio::select! {
                    message = close_rx.recv() => if message.is_none() { break },
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                }
            }
        });
        Self {
            stop,
            paused: Arc::new(AtomicBool::new(true)),
            spent: Arc::new(Mutex::new(U256::ZERO)),
            feed: Arc::new(Mutex::new(FeedHealth::default())),
            clock: Arc::new(ChainClock::default()),
            rules: Arc::new(Mutex::new(SnipeRules::default())),
            close_tx,
            positions,
            task: tokio::sync::Mutex::new(Some(task)),
        }
    }
}

pub async fn start_engine(
    rpc: Arc<Rpc>,
    cfg: Config,
    rules: SnipeRules,
    live: bool,
    start_paused: bool,
    emit: Emit,
) -> anyhow::Result<EngineHandle> {
    rules.validate()?;
    let state = StateDb::open("data")?;
    let positions = Arc::new(PositionStore::from_state(state.clone())?);
    let journal = Arc::new(TxJournal::from_state(state)?);
    let wallet = if live {
        Some(Arc::new(Wallet::require()?))
    } else {
        None
    };
    if let Some(wallet) = &wallet {
        let recovered = journal.recover(&rpc, &positions, wallet.address()).await?;
        anyhow::ensure!(
            recovered.blocked.is_empty(),
            "unresolved transaction recovery blocked live startup: {}",
            recovered.blocked.join("; ")
        );
        if recovered.recovered_entries > 0 || recovered.recovered_exits > 0 || recovered.failed > 0
        {
            warn(format!(
                "transaction recovery: {} entries, {} exits, {} no-fill operations",
                recovered.recovered_entries, recovered.recovered_exits, recovered.failed
            ));
        }
    }
    let clock = Arc::new(ChainClock::default());
    match rpc.latest_header(Lane::Hot).await {
        Ok(header) => clock.note_header(header.timestamp, now_ms(), header.base_fee, header.number),
        Err(error) if live => return Err(error.context("seed live chain clock")),
        Err(_) => {}
    }
    let submitter = if live {
        let submitter = Arc::new(Submitter::new(&cfg)?);
        let ips = submitter.resolve_and_pin().await?;
        anyhow::ensure!(!ips.is_empty(), "sequencer DNS returned no addresses");
        // One nonce allocator for the whole engine, seeded once. Sends allocate
        // from it; a mid-flight reset could double-spend a nonce.
        if let Some(wallet) = &wallet {
            wallet.set_nonce(
                rpc.get_transaction_count(Lane::Hot, wallet.address(), true)
                    .await?,
            );
        }
        Some(submitter)
    } else {
        None
    };
    let stop = Arc::new(AtomicBool::new(false));
    let paused = Arc::new(AtomicBool::new(start_paused));
    let spent = Arc::new(Mutex::new(U256::ZERO));
    let feed = Arc::new(Mutex::new(FeedHealth::default()));
    let rules = Arc::new(Mutex::new(rules));
    let (close_tx, close_rx) = mpsc::channel::<String>(32);
    let run_clock = clock.clone();
    let task = tokio::spawn({
        let stop = stop.clone();
        let paused = paused.clone();
        let spent = spent.clone();
        let feed = feed.clone();
        let rules = rules.clone();
        let positions = positions.clone();
        let journal = journal.clone();
        async move {
            run_loop(
                rpc,
                cfg,
                rules,
                live,
                stop.clone(),
                paused.clone(),
                spent,
                feed,
                emit,
                close_rx,
                wallet,
                positions,
                journal,
                run_clock,
                submitter,
            )
            .await;
            halt(&stop, &paused);
        }
    });
    Ok(EngineHandle {
        stop,
        paused,
        spent,
        feed,
        clock,
        rules,
        close_tx,
        positions,
        task: tokio::sync::Mutex::new(Some(task)),
    })
}

struct CloseGuard {
    id: String,
    closing: Arc<Mutex<HashSet<String>>>,
    keep: bool,
}

impl CloseGuard {
    fn try_new(id: &str, closing: &Arc<Mutex<HashSet<String>>>) -> Option<Self> {
        if closing.lock().insert(id.to_string()) {
            Some(Self {
                id: id.to_string(),
                closing: closing.clone(),
                keep: false,
            })
        } else {
            None
        }
    }
    fn keep(&mut self) {
        self.keep = true;
    }
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
        if !self.keep {
            self.closing.lock().remove(&self.id);
        }
    }
}

/// Atomic reservation of session budget + an open-position slot. Dropping it
/// releases both unless `commit` was called — every abort path stays correct.
struct EntryResv {
    spent: Arc<Mutex<U256>>,
    open_res: Arc<AtomicU64>,
    cost: U256,
    committed: bool,
    retained: bool,
}

impl EntryResv {
    /// The position now counts itself: keep `booked` of the reserved spend
    /// (releasing any refund) and drop the slot reservation.
    fn commit(mut self, booked: U256) {
        {
            let mut spent = self.spent.lock();
            if booked >= self.cost {
                *spent = spent.saturating_add(booked - self.cost);
            } else {
                *spent = spent.saturating_sub(self.cost - booked);
            }
        }
        self.committed = true;
    }

    /// An ambiguous send keeps its reservation — the tx may still land, and
    /// spending that budget twice is worse than holding it for the session.
    fn keep_all(mut self) {
        self.retained = true;
    }
}

impl Drop for EntryResv {
    fn drop(&mut self) {
        if self.retained {
            return;
        }
        self.open_res.fetch_sub(1, Ordering::SeqCst);
        if !self.committed {
            let mut s = self.spent.lock();
            *s = s.saturating_sub(self.cost);
        }
    }
}

/// Reserve entry value plus worst-case burst gas and one position slot atomically.
/// `decide` saw a stale snapshot; this is the check that actually binds.
fn try_reserve(
    spent: &Arc<Mutex<U256>>,
    open_res: &Arc<AtomicU64>,
    positions: &PositionStore,
    rules: &SnipeRules,
    cost: U256,
) -> Option<EntryResv> {
    let open_now = positions.open_positions().len();
    let prior = open_res.fetch_add(1, Ordering::SeqCst);
    if open_now + prior as usize >= rules.max_open_positions {
        open_res.fetch_sub(1, Ordering::SeqCst);
        return None;
    }
    {
        let mut reserved = spent.lock();
        if cost > rules.session_budget_wei.saturating_sub(*reserved) {
            open_res.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        *reserved = reserved.saturating_add(cost);
    }
    Some(EntryResv {
        spent: spent.clone(),
        open_res: open_res.clone(),
        cost,
        committed: false,
        retained: false,
    })
}

fn finish_failed_exit(
    positions: &PositionStore,
    journal: &TxJournal,
    operation_id: &str,
    position_id: &str,
) -> anyhow::Result<U256> {
    let gas_wei = journal.recorded_gas(operation_id)?;
    if !gas_wei.is_zero() {
        positions
            .update_with(position_id, |position| {
                position.charge_overhead_gas(operation_id, gas_wei)
            })
            .ok_or_else(|| {
                anyhow::anyhow!("failed exit references missing position {position_id}")
            })?;
        positions.flush()?;
    }
    journal.record(operation_id, JournalEvent::Failed)?;
    Ok(gas_wei)
}

fn launch_insiders(intel: &crate::pons::enrich::LaunchIntel) -> Vec<Address> {
    let mut insiders = intel
        .tx
        .as_ref()
        .map(|transaction| transaction.exemptions.clone())
        .unwrap_or_default();
    insiders.push(intel.ev.deployer);
    if let Some(transaction) = &intel.tx {
        insiders.push(transaction.recipient);
    }
    if let Some(record) = &intel.record {
        insiders.push(record.creator_fee_recipient);
    }
    insiders
}

async fn sync_flow(
    rpc: &Rpc,
    tracker: &Arc<Mutex<FlowTracker>>,
    curve: Address,
    lane: Lane,
) -> anyhow::Result<crate::pons::stream::FlowSnapshot> {
    let mut plan = tracker
        .lock()
        .sync(curve)
        .ok_or_else(|| anyhow::anyhow!("curve {curve:#x} is not watched"))?;
    if let Some((block, expected_hash)) = plan.anchor
        && rpc.block_hash(lane, block).await? != expected_hash
    {
        tracker.lock().rewind(curve, plan.origin_block);
        plan = tracker
            .lock()
            .sync(curve)
            .expect("rewound watched curve still exists");
    }
    let head = rpc.block_number(lane).await?;
    anyhow::ensure!(
        head >= plan.origin_block,
        "chain head {head} is behind flow origin {}",
        plan.origin_block
    );
    if head >= plan.from_block {
        let logs =
            crate::pons::stream::fetch_curve_logs_on(rpc, lane, curve, plan.from_block, head)
                .await?;
        let head_hash = rpc.block_hash(lane, head).await?;
        tracker
            .lock()
            .apply_range(curve, plan.from_block, head, head_hash, &logs)?;
    }
    Ok(tracker.lock().snapshot(curve))
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    rpc: Arc<Rpc>,
    cfg: Config,
    rules: Arc<Mutex<SnipeRules>>,
    live: bool,
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    spent: Arc<Mutex<U256>>,
    feed: Arc<Mutex<FeedHealth>>,
    emit: Emit,
    mut close_rx: mpsc::Receiver<String>,
    wallet: Option<Arc<Wallet>>,
    positions: Arc<PositionStore>,
    journal: Arc<TxJournal>,
    clock: Arc<ChainClock>,
    submitter: Option<Arc<Submitter>>,
) {
    let recipient = wallet
        .as_ref()
        .map(|w| w.address())
        .unwrap_or(crate::chain::DEAD);
    let outcomes = Arc::new(OutcomeLog::open("data"));
    let clock_task = spawn_clock(rpc.clone(), clock.clone(), cfg.rpc_ws.first().cloned()).await;
    let refresh_task = submitter.as_ref().map(Submitter::spawn_refresh);
    let burst = Arc::new(BurstCtl::from_env());
    let limiter = Arc::new(Limiter::new(3));
    let index: SharedIndex = Arc::new(tokio::sync::Mutex::new(
        crate::pons::deployer::DeployerIndex::default(),
    ));
    let index_task = {
        let rpc = rpc.clone();
        let index = index.clone();
        tokio::spawn(async move {
            // Two-day deployer history + which of those tokens graduated.
            let (evs, grads) = tokio::join!(
                crate::pons::launches::recent_launches(&rpc, 1_728_000, None, None),
                crate::pons::launches::recent_graduations(&rpc, 1_728_000),
            );
            let mut idx = index.lock().await;
            if let Ok(evs) = evs {
                for ev in &evs {
                    idx.note(ev);
                }
            }
            if let Ok(grads) = grads {
                let mut top = 0u64;
                for (token, b) in &grads {
                    idx.mark_graduated(*token);
                    top = top.max(*b);
                }
                idx.set_last_grad_block(top);
            }
            idx.mark_ready();
        })
    };
    // Slow loop keeps the graduation set current without re-scanning.
    let grad_task = {
        let rpc = rpc.clone();
        let index = index.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                iv.tick().await;
                let Ok(head) = rpc.block_number(Lane::Background).await else {
                    continue;
                };
                let from = index
                    .lock()
                    .await
                    .last_grad_block()
                    .saturating_sub(2_000)
                    .max(head.saturating_sub(50_000));
                let Ok(logs) = rpc
                    .get_logs(
                        Lane::Background,
                        alloy::rpc::types::Filter::new()
                            .address(crate::chain::ADDR.pons_factory)
                            .event_signature(crate::abi::topics::pool_graduated())
                            .from_block(from)
                            .to_block(head),
                    )
                    .await
                else {
                    continue;
                };
                let mut idx = index.lock().await;
                for l in &logs {
                    if let Ok(g) = crate::abi::factory::PoolGraduated::decode_log(&l.clone().into())
                    {
                        idx.mark_graduated(g.token);
                        if let Some(b) = l.block_number {
                            idx.set_last_grad_block(b);
                        }
                    }
                }
            }
        })
    };
    let (tx, mut rx) = mpsc::channel::<LaunchEvent>(1_024);
    let watch = watch_launches(rpc.clone(), cfg.clone(), tx, feed.clone());
    let farms = Arc::new(Mutex::new(FarmDetector::default()));
    let flow = Arc::new(Mutex::new(FlowTracker::default()));
    for position in
        positions.open_positions_for(!live, wallet.as_ref().map(|wallet| wallet.address()))
    {
        let Ok(Some(event)) = find_launch(&rpc, position.token).await else {
            continue;
        };
        let intel = enrich_launch(&rpc, event.clone(), recipient).await;
        if let Some(curve) = &intel.curve {
            flow.lock().watch(
                event.curve,
                curve.launched_at,
                event.block_number,
                launch_insiders(&intel),
            );
        }
    }
    let busy = Arc::new(Mutex::new(HashSet::<Address>::new()));
    let open_res = Arc::new(AtomicU64::new(0));
    let execution = Arc::new(tokio::sync::Mutex::new(()));
    // Positions with a sell in flight — the manage loop and manual close
    // must never double-sell the same bag.
    let closing = Arc::new(Mutex::new(HashSet::<String>::new()));
    let managing = Arc::new(AtomicBool::new(false));

    let manage_task = {
        let positions = positions.clone();
        let rpc = rpc.clone();
        let wallet = wallet.clone();
        let rules = rules.clone();
        let emit = emit.clone();
        let flow = flow.clone();
        let managing = managing.clone();
        let submitter = submitter.clone();
        let clock = clock.clone();
        let closing = closing.clone();
        let execution = execution.clone();
        let journal = journal.clone();
        let outcomes = outcomes.clone();
        let index = index.clone();
        let stop = stop.clone();
        let paused = paused.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(1000));
            let mut n = 0u64;
            while !stop.load(Ordering::SeqCst) {
                tick.tick().await;
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                n += 1;
                if live
                    && n.is_multiple_of(10)
                    && let Some(wallet) = &wallet
                {
                    match journal.canonical_issues(&rpc, wallet.address()).await {
                        Ok(issues) if !issues.is_empty() => {
                            halt(&stop, &paused);
                            emit(
                                serde_json::json!({"kind":"engine_error","message":format!("canonical transaction check failed: {}", issues.join("; "))}),
                            );
                            break;
                        }
                        Err(error) => {
                            paused.store(true, Ordering::SeqCst);
                            emit(
                                serde_json::json!({"kind":"engine_error","message":format!("canonical transaction check unavailable: {}", crate::fmt::first_line(&error.to_string()))}),
                            );
                        }
                        _ => {}
                    }
                }
                if managing.swap(true, Ordering::SeqCst) {
                    continue;
                }
                for stale in
                    positions.open_positions_for(!live, wallet.as_ref().map(|w| w.address()))
                {
                    let tokens = stale.held();
                    if tokens.is_zero() {
                        continue;
                    }
                    let age = (now_ms() / 1000).saturating_sub(stale.opened_at);
                    if age >= 60 && !n.is_multiple_of(30) {
                        continue;
                    }
                    let Some(mut guard) = CloseGuard::try_new(&stale.id, &closing) else {
                        continue;
                    };
                    let Some(pos) = positions.load().into_iter().find(|p| p.id == stale.id) else {
                        continue;
                    };
                    if pos.status != "open" {
                        continue;
                    }
                    let tokens = pos.held();
                    if tokens.is_zero() {
                        continue;
                    }
                    let Ok(val) = value_now(&rpc, pos.token, tokens).await else {
                        continue;
                    };
                    let Some(val) = val else { continue };
                    let basis = pos.basis();
                    let pnl = pos.net_pnl_pct(val.eth);
                    let peak: U256 = pos.peak_eth.parse().unwrap_or(val.eth);
                    let peak = peak.max(val.eth);
                    let _ = positions.update(&pos.id, |p| {
                        p.last_eth = val.eth.to_string();
                        p.last_at = now_ms() / 1000;
                        p.peak_eth = peak.to_string();
                    });
                    let graduated = val.phase == 2;
                    let snap = if graduated {
                        flow.lock().snapshot(pos.curve)
                    } else {
                        match sync_flow(&rpc, &flow, pos.curve, Lane::Hot).await {
                            Ok(snapshot) => snapshot,
                            Err(error) => {
                                emit(
                                    serde_json::json!({"kind":"exit_error","positionId":pos.id,"message":format!("flow sync failed: {}", crate::fmt::first_line(&error.to_string())),"pending":false}),
                                );
                                continue;
                            }
                        }
                    };
                    let rules_now = rules.lock().clone();
                    if let Some(act) = pick_exit(
                        &pos,
                        val.eth,
                        &rules_now,
                        Some(&snap),
                        now_ms() / 1000,
                        graduated,
                        val.progress,
                    ) {
                        let qty = tokens * U256::from(act.fraction_bps) / U256::from(10_000u64);
                        let _execution_guard = if live {
                            Some(execution.lock().await)
                        } else {
                            None
                        };
                        let operation_id = if live {
                            let Some(w) = &wallet else { continue };
                            match journal.begin(
                                w.address(),
                                OperationSpec::Exit {
                                    position_id: pos.id.clone(),
                                    token: pos.token,
                                    curve: pos.curve,
                                    reason: act.reason.clone(),
                                },
                            ) {
                                Ok(id) => Some(id),
                                Err(e) => {
                                    guard.keep();
                                    halt(&stop, &paused);
                                    emit(
                                        serde_json::json!({"kind":"exit_error","positionId":pos.id,"message":e.to_string(),"pending":true}),
                                    );
                                    continue;
                                }
                            }
                        } else {
                            None
                        };
                        let exec = match (&submitter, &wallet, &operation_id) {
                            (Some(s), Some(w), Some(operation_id)) if live => Some(
                                LiveExec::new(&rpc, s, w, &clock)
                                    .with_operation(journal.clone(), operation_id.clone()),
                            ),
                            _ => None,
                        };
                        let result = sell_anywhere(
                            &rpc,
                            exec.as_ref(),
                            pos.token,
                            qty,
                            rules_now.slippage_bps,
                            !live,
                        )
                        .await;
                        match result {
                            Ok(res) => {
                                // eth_out is actual (live) or the quote (dry).
                                let out = match res.eth_out {
                                    Some(o) => o,
                                    None if !live => res.eth_quoted,
                                    None => {
                                        guard.keep();
                                        halt(&stop, &paused);
                                        let message =
                                            "confirmed sell returned no eth_out — position kept";
                                        warn(format!("exit {}: {message}", pos.symbol));
                                        emit(
                                            serde_json::json!({"kind":"exit_error","positionId":pos.id,"message":message,"pending":true}),
                                        );
                                        continue;
                                    }
                                };
                                let Some((updated, applied)) =
                                    positions.update_with(&pos.id, |p| {
                                        p.apply_exit(
                                            res.tokens_in,
                                            out,
                                            res.gas_wei,
                                            act.reason.clone(),
                                            res.hash,
                                            !live,
                                            now_ms() / 1000,
                                        )
                                    })
                                else {
                                    guard.keep();
                                    halt(&stop, &paused);
                                    emit(
                                        serde_json::json!({"kind":"exit_error","positionId":pos.id,"message":"confirmed exit references a missing position","pending":true}),
                                    );
                                    continue;
                                };
                                if applied.tokens_sold.is_zero() {
                                    guard.keep();
                                    halt(&stop, &paused);
                                    emit(
                                        serde_json::json!({"kind":"exit_error","positionId":pos.id,"message":"confirmed exit could not be applied to inventory","pending":true}),
                                    );
                                    continue;
                                }
                                if applied.closed {
                                    flow.lock().unwatch(pos.curve);
                                }
                                if res.venue == "pool" {
                                    index.lock().await.mark_graduated(pos.token);
                                }
                                match positions.flush() {
                                    Ok(()) => {
                                        if let Some(operation_id) = &operation_id
                                            && let Err(e) =
                                                journal.record(operation_id, JournalEvent::Applied)
                                        {
                                            guard.keep();
                                            halt(&stop, &paused);
                                            emit(
                                                serde_json::json!({"kind":"engine_error","message":format!("transaction journal: {e}")}),
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        guard.keep();
                                        halt(&stop, &paused);
                                        emit(
                                            serde_json::json!({"kind":"engine_error","message":format!("positions flush: {e}")}),
                                        );
                                    }
                                }
                                emit(serde_json::json!({
                                    "kind":"exit","positionId":pos.id,"symbol":pos.symbol,
                                    "ethOut":out.to_string(),"reason":act.reason,"venue":res.venue,
                                    "closed":applied.closed,
                                    "pnlPct": if applied.closed { updated.net_pnl_pct(updated.realized_out()) } else { updated.net_pnl_pct(updated.last_eth.parse().unwrap_or_default()) },
                                    "realizedWei":applied.net_realized,
                                    "realizedBeforeGasWei":applied.realized,
                                    "gasWei":res.gas_wei.to_string(),
                                    "realizedTotalWei":updated.net_realized_pnl_wei(),
                                    "realizedTotalBeforeGasWei":updated.realized_pnl_wei(),
                                    "closedAt":applied.closed.then_some(now_ms()),
                                    "ethIn":updated.entry_eth,
                                    "tokensLeft":applied.remaining.to_string(),
                                    "basisWei":updated.basis().to_string(),
                                }));
                                outcomes.write(
                                    OutcomeKind::Exit,
                                    serde_json::json!({
                                        "token": format!("{:#x}", pos.token),
                                        "reason": act.reason, "venue": res.venue,
                                        "tokens_in": applied.tokens_sold.to_string(), "eth_out": out.to_string(),
                                        "gas_wei": res.gas_wei.to_string(), "realized_wei": applied.net_realized,
                                        "realized_before_gas_wei": applied.realized,
                                        "closed": applied.closed, "tx": res.hash.map(|h| format!("{h:#x}")),
                                        "live": live,
                                    }),
                                );
                            }
                            Err(e) => {
                                let mut pending = operation_id
                                    .as_ref()
                                    .is_some_and(|id| journal.requires_recovery(id));
                                let mut failed_gas = U256::ZERO;
                                if let Some(operation_id) = &operation_id
                                    && !pending
                                {
                                    match finish_failed_exit(
                                        &positions,
                                        &journal,
                                        operation_id,
                                        &pos.id,
                                    ) {
                                        Ok(gas) => failed_gas = gas,
                                        Err(_) => pending = true,
                                    }
                                }
                                if pending {
                                    guard.keep();
                                    halt(&stop, &paused);
                                }
                                let message = crate::fmt::first_line(&e.to_string());
                                warn(format!("exit {}: {message}", pos.symbol));
                                emit(
                                    serde_json::json!({"kind":"exit_error","positionId":pos.id,"message":message,"pending":pending,"gasWei":failed_gas.to_string()}),
                                );
                                outcomes.write(
                                    OutcomeKind::ExitFailed,
                                    serde_json::json!({"token":format!("{:#x}",pos.token),"position_id":pos.id,"gas_wei":failed_gas.to_string(),"pending":pending,"live":live,"reason":message}),
                                );
                            }
                        }
                    } else {
                        emit(serde_json::json!({
                            "kind":"mark","positionId":pos.id,"symbol":pos.symbol,
                            "valueEth":val.eth.to_string(),"venue":val.venue,
                            "pnlPct":pnl,"peakPct":pnl_pct(peak, basis),
                        }));
                    }
                }
                positions.flush_if_due();
                managing.store(false, Ordering::SeqCst);
            }
        })
    };

    let mut entries = tokio::task::JoinSet::new();
    let entry_slots = Arc::new(tokio::sync::Semaphore::new(64));
    while !stop.load(Ordering::SeqCst) {
        tokio::select! {
            Some(id) = close_rx.recv() => {
                let Some(mut guard) = CloseGuard::try_new(&id, &closing) else {
                    emit(serde_json::json!({"kind":"close_error","positionId":id,"message":"a close is already in progress","pending":true}));
                    continue;
                };
                // Only positions this mode owns may close through the engine.
                let pos = positions
                    .open_positions_for(!live, wallet.as_ref().map(|w| w.address()))
                    .into_iter()
                    .find(|p| p.id == id);
                let Some(pos) = pos else {
                    emit(serde_json::json!({"kind":"close_error","positionId":id,"message":"no open position with that id in this mode"}));
                    continue;
                };
                let tokens = pos.held();
                let slip = rules.lock().slippage_bps;
                let _execution_guard = if live { Some(execution.lock().await) } else { None };
                let operation_id = if live {
                    let Some(w) = &wallet else { continue };
                    match journal.begin(
                        w.address(),
                        OperationSpec::Exit {
                            position_id: pos.id.clone(),
                            token: pos.token,
                            curve: pos.curve,
                            reason: "closed by hand".into(),
                        },
                    ) {
                        Ok(id) => Some(id),
                        Err(e) => {
                            guard.keep();
                            halt(&stop, &paused);
                            emit(serde_json::json!({"kind":"close_error","positionId":pos.id,"message":e.to_string(),"pending":true}));
                            continue;
                        }
                    }
                } else {
                    None
                };
                let exec = match (&submitter, &wallet, &operation_id) {
                    (Some(s), Some(w), Some(operation_id)) if live => Some(
                        LiveExec::new(&rpc, s, w, &clock).with_operation(journal.clone(), operation_id.clone()),
                    ),
                    _ => None,
                };
                let result = sell_anywhere(&rpc, exec.as_ref(), pos.token, tokens, slip, !live).await;
                match result {
                    Ok(res) => {
                        let out = match res.eth_out {
                            Some(o) => o,
                            None if !live => res.eth_quoted,
                            None => {
                                guard.keep();
                                halt(&stop, &paused);
                                emit(serde_json::json!({"kind":"close_error","positionId":pos.id,"message":"confirmed sell returned no eth_out","pending":true}));
                                continue;
                            }
                        };
                        let Some((updated, applied)) = positions.update_with(&pos.id, |p| {
                            p.apply_exit(res.tokens_in, out, res.gas_wei, "closed by hand".into(), res.hash, !live, now_ms() / 1000)
                        }) else {
                            guard.keep();
                            halt(&stop, &paused);
                            emit(serde_json::json!({"kind":"close_error","positionId":pos.id,"message":"confirmed close references a missing position","pending":true}));
                            continue;
                        };
                        if applied.tokens_sold.is_zero() {
                            guard.keep();
                            halt(&stop, &paused);
                            emit(serde_json::json!({"kind":"close_error","positionId":pos.id,"message":"confirmed close could not be applied to inventory","pending":true}));
                            continue;
                        }
                        if applied.closed {
                            flow.lock().unwatch(pos.curve);
                        }
                        if res.venue == "pool" {
                            index.lock().await.mark_graduated(pos.token);
                        }
                        match positions.flush() {
                            Ok(()) => {
                                if let Some(operation_id) = &operation_id
                                    && let Err(e) = journal.record(operation_id, JournalEvent::Applied)
                                {
                                    guard.keep();
                                    halt(&stop, &paused);
                                    emit(serde_json::json!({"kind":"engine_error","message":format!("transaction journal: {e}")}));
                                }
                            }
                            Err(e) => {
                                guard.keep();
                                halt(&stop, &paused);
                                emit(serde_json::json!({"kind":"engine_error","message":format!("positions flush: {e}")}));
                            }
                        }
                        emit(serde_json::json!({
                            "kind":"exit","positionId":pos.id,"symbol":pos.symbol,
                            "ethOut":out.to_string(),"reason":"closed by hand","venue":res.venue,
                            "closed":applied.closed,
                            "pnlPct": if applied.closed { updated.net_pnl_pct(updated.realized_out()) } else { updated.net_pnl_pct(updated.last_eth.parse().unwrap_or_default()) },
                            "realizedWei":applied.net_realized,
                            "realizedBeforeGasWei":applied.realized,
                            "gasWei":res.gas_wei.to_string(),
                            "realizedTotalWei":updated.net_realized_pnl_wei(),
                            "realizedTotalBeforeGasWei":updated.realized_pnl_wei(),
                            "closedAt":applied.closed.then_some(now_ms()),
                            "ethIn":updated.entry_eth,
                            "tokensLeft":applied.remaining.to_string(),
                            "basisWei":updated.basis().to_string(),
                        }));
                        outcomes.write(
                            OutcomeKind::Exit,
                            serde_json::json!({
                                "token": format!("{:#x}", pos.token), "reason": "closed by hand",
                                "venue": res.venue, "tokens_in": applied.tokens_sold.to_string(),
                                "eth_out": out.to_string(), "gas_wei": res.gas_wei.to_string(),
                                "realized_wei": applied.net_realized,
                                "realized_before_gas_wei": applied.realized,
                                "closed": applied.closed, "tx": res.hash.map(|h| format!("{h:#x}")), "live": live,
                            }),
                        );
                    }
                    Err(e) => {
                        let mut pending = operation_id.as_ref().is_some_and(|id| journal.requires_recovery(id));
                        let mut failed_gas = U256::ZERO;
                        if let Some(operation_id) = &operation_id
                            && !pending
                        {
                            match finish_failed_exit(
                                &positions,
                                &journal,
                                operation_id,
                                &pos.id,
                            ) {
                                Ok(gas) => failed_gas = gas,
                                Err(_) => pending = true,
                            }
                        }
                        if pending {
                            guard.keep();
                            halt(&stop, &paused);
                        }
                        let message = crate::fmt::first_line(&e.to_string());
                        emit(serde_json::json!({"kind":"close_error","positionId":pos.id,"message":message,"pending":pending,"gasWei":failed_gas.to_string()}));
                        outcomes.write(
                            OutcomeKind::ExitFailed,
                            serde_json::json!({"token":format!("{:#x}",pos.token),"position_id":pos.id,"gas_wei":failed_gas.to_string(),"pending":pending,"live":live,"reason":message}),
                        );
                    }
                }
            }
            Some(result) = entries.join_next(), if !entries.is_empty() => {
                if let Err(e) = result {
                    warn(format!("entry task: {e}"));
                }
            }
            Some(ev) = rx.recv() => {
                let Ok(entry_permit) = entry_slots.clone().try_acquire_owned() else {
                    emit(serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["entry work queue full"]}));
                    continue;
                };
                let rpc = rpc.clone();
                let rules = rules.clone();
                let paused = paused.clone();
                let stop = stop.clone();
                let spent = spent.clone();
                let open_res = open_res.clone();
                let emit = emit.clone();
                let index = index.clone();
                let farms = farms.clone();
                let flow = flow.clone();
                let busy = busy.clone();
                let positions = positions.clone();
                let outcomes = outcomes.clone();
                let clock = clock.clone();
                let burst = burst.clone();
                let limiter = limiter.clone();
                let submitter = submitter.clone();
                let wallet = wallet.clone();
                let journal = journal.clone();
                let execution = execution.clone();
                let helper = cfg.helper;
                entries.spawn(async move {
                    let _entry_permit = entry_permit;
                    handle_launch(rpc, ev, recipient, rules, paused, stop, spent, open_res, emit, index, farms, flow, busy, positions, outcomes, clock, burst, limiter, submitter, wallet, journal, execution, helper, live).await;
                });
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
        }
    }
    watch.shutdown().await;
    paused.store(true, Ordering::SeqCst);
    while let Some(result) = entries.join_next().await {
        if let Err(e) = result {
            warn(format!("entry task: {e}"));
        }
    }
    if let Err(e) = manage_task.await {
        warn(format!("manage task: {e}"));
    }
    for task in [index_task, grad_task, clock_task] {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = refresh_task {
        task.abort();
        let _ = task.await;
    }
    if let Err(e) = positions.flush() {
        warn(format!("positions flush: {e}"));
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_launch(
    rpc: Arc<Rpc>,
    ev: LaunchEvent,
    recipient: Address,
    rules: Arc<Mutex<SnipeRules>>,
    paused: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    spent: Arc<Mutex<U256>>,
    open_res: Arc<AtomicU64>,
    emit: Emit,
    index: SharedIndex,
    farms: Arc<Mutex<FarmDetector>>,
    flow: Arc<Mutex<FlowTracker>>,
    busy: Arc<Mutex<HashSet<Address>>>,
    positions: Arc<PositionStore>,
    outcomes: Arc<OutcomeLog>,
    clock: Arc<ChainClock>,
    burst: Arc<BurstCtl>,
    limiter: Arc<Limiter>,
    submitter: Option<Arc<Submitter>>,
    wallet: Option<Arc<Wallet>>,
    journal: Arc<TxJournal>,
    execution: Arc<tokio::sync::Mutex<()>>,
    helper: Option<Address>,
    live: bool,
) {
    if !busy.lock().insert(ev.token) {
        return;
    }
    let t0 = now_ms();
    index.lock().await.note(&ev);
    let intel = limiter
        .run(|| enrich_launch(&rpc, ev.clone(), recipient))
        .await;
    let dq = index.lock().await.quick(ev.deployer, ev.block_number);
    let twins = farms.lock().note(&intel, now_ms()).0;
    let mut ctx = ScoreContext {
        deployer: dq,
        farm_twins: twins,
        ..Default::default()
    };
    if let Some(rec) = &intel.record {
        ctx.fee_recipient_is_contract = is_contract(&rpc, rec.creator_fee_recipient).await.ok();
    }
    let score = score_launch(&intel, &ctx);
    let rules_now = rules.lock().clone();
    let open = positions.open_positions().len();
    let mut d = decide(&intel, &score, &rules_now, open, twins, *spent.lock());
    let screen_fire = d.fire;
    let screen_why = d.why.clone();
    let launched_at = intel
        .curve
        .as_ref()
        .map(|c| c.launched_at)
        .or_else(|| intel.tx.as_ref().map(|t| t.timestamp))
        .unwrap_or(0);
    let start_bps = intel
        .curve
        .as_ref()
        .and_then(|c| c.snipe_tax_start_bps.try_into().ok())
        .unwrap_or(0);
    let window = intel
        .curve
        .as_ref()
        .and_then(|c| c.snipe_tax_seconds.try_into().ok())
        .unwrap_or(0);
    if d.fire && paused.load(Ordering::SeqCst) {
        d.why.push(if live {
            "not armed".into()
        } else {
            "demo not started".into()
        });
        d.fire = false;
    }
    let has_x = has_socials(intel.meta.as_ref()).twitter;
    emit(serde_json::json!({
        "kind":"launch","t": now_ms(),
        "token":format!("{:#x}",ev.token),"curve":format!("{:#x}",ev.curve),
        "symbol": intel.meta.as_ref().map(|m| format!("${}", m.symbol)).unwrap_or_default(),
        "name": intel.meta.as_ref().map(|m| m.name.clone()).unwrap_or_else(|| "(unreadable)".into()),
        "score": score.total, "verdict": score.verdict.as_str(), "fire": d.fire, "why": d.why,
        "devPct": dev_share_pct(intel.tx.as_ref()), "taxBps": intel.record.as_ref().map(|r| r.creator_tax_bps),
        "pair": intel.pair.symbol, "readMs": now_ms()-t0,
        "detail": {
            "reasons": score.reasons,
            "description": intel.meta.as_ref().map(|m| m.description.chars().take(280).collect::<String>()).unwrap_or_default(),
            "socials": {"x": if has_x { safe_url(intel.meta.as_ref().map(|m| m.socials.twitter.as_str()).unwrap_or("")) } else { String::new() }},
            "deployer": format!("{:#x}", ev.deployer),
            "deployerPrior": dq.map(|d| d.0), "deployerGraduated": dq.map(|d| d.1),
            "exempt": intel.tx.as_ref().map(|t| t.exemptions.iter().map(|a| format!("{a:#x}")).collect::<Vec<_>>()),
            "openingTaxBps": intel.curve.as_ref().map(|c| c.opening_tax_bps.to_string()),
            "block": ev.block_number.to_string(), "tx": format!("{:#x}", ev.tx_hash), "errors": intel.errors
        }
    }));
    outcomes.write(
        OutcomeKind::LaunchSeen,
        serde_json::json!({
            "token":format!("{:#x}",ev.token),"curve":format!("{:#x}",ev.curve),
            "launch_block":ev.block_number,"launched_at":launched_at,"start_bps":start_bps,"window":window,
            "score":score.total,"verdict":score.verdict.as_str(),"screen_fire":screen_fire,
            "screen_why":screen_why,"operational_fire":d.fire,
            "curve_state":intel.curve.as_ref(),
        }),
    );
    if !d.fire {
        busy.lock().remove(&ev.token);
        return;
    }
    if let Err(error) = clock.require_quality(2_000, 1_500) {
        emit(
            serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":[format!("clock quality: {error}")]}),
        );
        busy.lock().remove(&ev.token);
        return;
    }
    // Atomic budget + slot reservation — decide() saw a possibly-stale snapshot.
    let gas_reserve = if live {
        wallet
            .as_ref()
            .map(|wallet| wallet.worst_case_gas_wei(&clock, burst.max))
            .unwrap_or(U256::ZERO)
    } else {
        U256::ZERO
    };
    let risk_cost = rules_now.eth_per_buy.saturating_add(gas_reserve);
    let resv = match try_reserve(&spent, &open_res, &positions, &rules_now, risk_cost) {
        Some(r) => r,
        None => {
            emit(
                serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["budget/slot/gas reservation unavailable"]}),
            );
            busy.lock().remove(&ev.token);
            return;
        }
    };
    if let Some(curve) = &intel.curve {
        flow.lock().watch(
            ev.curve,
            curve.launched_at,
            ev.block_number,
            launch_insiders(&intel),
        );
    }
    let boundary = boundary_instant(launched_at, rules_now.entry_second);
    let chain_now = clock.sequencer_now();
    if boundary <= chain_now {
        // A late event must never fire "on schedule" — the window is gone.
        emit(
            serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["entry window already passed"]}),
        );
        busy.lock().remove(&ev.token);
        return;
    }
    if (boundary - chain_now).saturating_mul(1000) > rules_now.max_wait_ms {
        emit(
            serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["entry boundary too far out — stale clock or replayed event"]}),
        );
        busy.lock().remove(&ev.token);
        return;
    }
    clock.sleep_until_unix(boundary.saturating_sub(1)).await;
    // The cursor verifies its previous canonical block before extending a
    // contiguous range. A gap or reorg rebuilds from the launch block.
    let snap = match sync_flow(&rpc, &flow, ev.curve, Lane::Hot).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            emit(
                serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":[format!("flow sync failed: {}",crate::fmt::first_line(&error.to_string()))]}),
            );
            busy.lock().remove(&ev.token);
            return;
        }
    };
    outcomes.write(
        OutcomeKind::GateObserved,
        serde_json::json!({
            "token":format!("{:#x}",ev.token),"taxed_buyers_s1":snap.taxed_buyers_s1,
            "exempt_buys_s0":snap.exempt_buys_s0,"insider_sold":snap.insider_sold,
            "flow":flow.lock().records(ev.curve),
        }),
    );
    let g = live_gate(&rules_now, &snap);
    if !g.fire {
        info(format!(
            "{}  hold  {}",
            muted(crate::fmt::hhmmss(None)),
            g.why.join("; ")
        ));
        emit(serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":g.why}));
        busy.lock().remove(&ev.token);
        return;
    }
    // Re-check the armed flag immediately before any send path.
    if paused.load(Ordering::SeqCst) {
        emit(
            serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["paused before send"]}),
        );
        busy.lock().remove(&ev.token);
        return;
    }
    let tax = snipe_tax_bps(start_bps, window, rules_now.entry_second);
    if !live {
        // Dry entries wait for the real +2 boundary like live ones — the
        // recorded open time is the moment a live burst would have fired.
        clock.sleep_until_unix(boundary).await;
        if paused.load(Ordering::SeqCst) || clock.require_quality(2_000, 1_500).is_err() {
            emit(
                serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["paused or chain clock unhealthy before entry"]}),
            );
            busy.lock().remove(&ev.token);
            return;
        }
        let fresh = match crate::trade::curve::read_curve_state(&rpc, ev.curve, recipient).await {
            Ok(s) => s,
            Err(e) => {
                emit(
                    serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":[format!("curve re-read before entry: {}", crate::fmt::first_line(&e.to_string()))]}),
                );
                busy.lock().remove(&ev.token);
                return;
            }
        };
        if fresh.graduated || fresh.ready_to_graduate {
            emit(
                serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["curve closed or graduated before entry"]}),
            );
            busy.lock().remove(&ev.token);
            return;
        }
        let q = crate::pons::curve::quote_buy(&fresh, rules_now.eth_per_buy);
        if q.tokens_out.is_zero() {
            emit(
                serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["simulated entry buys zero tokens at current tax"]}),
            );
            busy.lock().remove(&ev.token);
            return;
        }
        let tax: u64 = fresh.opening_tax_bps.try_into().unwrap_or(tax);
        let sh = shadow_result(launched_at, rules_now.entry_second);
        let pos = positions.open_position(
            ev.token,
            ev.curve,
            intel
                .meta
                .as_ref()
                .map(|m| m.symbol.clone())
                .unwrap_or_else(|| "?".into()),
            intel
                .meta
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_else(|| "?".into()),
            now_ms() / 1000,
            None,
            true,
            q.spent,
            U256::ZERO,
            q.tokens_out,
            crate::chain::CHAIN_ID,
            wallet.as_ref().map(|w| w.address()),
        );
        resv.commit(q.spent);
        if let Err(e) = positions.flush() {
            paused.store(true, Ordering::SeqCst);
            emit(
                serde_json::json!({"kind":"engine_error","message":format!("positions flush: {e}")}),
            );
        }
        info(format!(
            "{}  {} would buy {} ETH at tax {}  {}",
            muted(crate::fmt::hhmmss(None)),
            on_neon(" FIRE "),
            crate::fmt::eth(q.spent),
            crate::fmt::bps(tax),
            muted(&sh.message)
        ));
        emit(
            serde_json::json!({"kind":"fire","token":format!("{:#x}",ev.token),"taxBps":tax,"waitedMs":now_ms()-t0,"live":false,"simulated":true,"positionId":pos.id,"ethIn":q.spent.to_string(),"tokens":q.tokens_out.to_string(),"symbol":pos.symbol}),
        );
        outcomes.write(
            OutcomeKind::Fire,
            serde_json::json!({"token": format!("{:#x}", ev.token), "tax": tax, "simulated": true}),
        );
        return;
    }
    let (Some(helper), Some(wallet), Some(sub)) = (helper, wallet, submitter) else {
        warn("live burst needs HELPER_ADDRESS, PRIVATE_KEY and sequencer");
        emit(
            serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["live entry needs helper, key and sequencer"]}),
        );
        busy.lock().remove(&ev.token);
        return;
    };
    let _execution_guard = execution.lock().await;
    if paused.load(Ordering::SeqCst)
        || stop.load(Ordering::SeqCst)
        || clock.sequencer_now() >= boundary
    {
        emit(
            serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["execution lane was not available before the entry boundary"]}),
        );
        busy.lock().remove(&ev.token);
        return;
    }
    let min_out = intel
        .curve
        .as_ref()
        .map(|c| {
            crate::pons::curve::min_out_as_last_in_block(
                c,
                rules_now.eth_per_buy,
                U256::from(100_000_000_000_000_000u128),
                rules_now.slippage_bps,
            )
        })
        .unwrap_or(U256::ZERO);
    let data = encode_buy_once(
        ev.curve,
        ev.token,
        recipient,
        U256::from(rules_now.max_opening_tax_bps),
        min_out,
        U256::from(4_200_000_000_000_000_000u128),
    );
    let me = wallet.address();
    let symbol = intel
        .meta
        .as_ref()
        .map(|m| m.symbol.clone())
        .unwrap_or_else(|| "?".into());
    let name = intel
        .meta
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_else(|| "?".into());
    let operation_id = match journal.begin(
        me,
        OperationSpec::Entry {
            token: ev.token,
            curve: ev.curve,
            symbol: symbol.clone(),
            name: name.clone(),
            value: rules_now.eth_per_buy.to_string(),
        },
    ) {
        Ok(id) => id,
        Err(e) => {
            halt(&stop, &paused);
            emit(
                serde_json::json!({"kind":"engine_error","message":format!("transaction journal: {e}")}),
            );
            return;
        }
    };
    match presign_burst(
        &wallet,
        &clock,
        helper,
        data,
        rules_now.eth_per_buy,
        burst.max,
    ) {
        Ok(txs) => {
            if let Err(e) = journal.record(
                &operation_id,
                JournalEvent::StageMany {
                    step: "entry".into(),
                    txs: txs.iter().map(|tx| (tx.nonce, tx.hash)).collect(),
                },
            ) {
                let start = txs
                    .first()
                    .map(|tx| tx.nonce)
                    .unwrap_or_else(|| wallet.peek_nonce());
                wallet.rewind_nonces(start, txs.len() as u64);
                halt(&stop, &paused);
                emit(
                    serde_json::json!({"kind":"engine_error","message":format!("transaction journal: {e}")}),
                );
                return;
            }
            let res = clock_fire(
                &clock,
                launched_at,
                rules_now.entry_second,
                burst.lead_ms(),
                &sub,
                &txs,
                Some(&paused),
            )
            .await;
            let latency = sub.latency();
            outcomes.write(
                OutcomeKind::Submission,
                serde_json::json!({
                    "token": format!("{:#x}", ev.token), "class": format!("{:?}", res.class),
                    "attempt": res.attempt, "hash": res.hash.map(|h| format!("{h:#x}")),
                    "latency_count":latency.count,"latency_p50_us":latency.p50_us,
                    "latency_p95_us":latency.p95_us,"latency_p99_us":latency.p99_us,
                }),
            );
            if res.class == BurstClass::Aborted {
                let start = txs
                    .first()
                    .map(|tx| tx.nonce)
                    .unwrap_or_else(|| wallet.peek_nonce());
                let rewound = wallet.rewind_nonces(start, txs.len() as u64);
                let recorded = journal
                    .record(&operation_id, JournalEvent::AllRejected)
                    .and_then(|_| journal.record(&operation_id, JournalEvent::Failed));
                if !rewound || recorded.is_err() {
                    halt(&stop, &paused);
                    emit(
                        serde_json::json!({"kind":"engine_error","message":"paused burst could not cleanly release its nonce or journal reservation; restart before sending again"}),
                    );
                }
                emit(
                    serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":["paused before send"]}),
                );
            } else {
                let reconciliation =
                    reconcile_burst(&rpc, &journal, &operation_id, &txs, &res.outcomes).await;
                let mut unresolved = reconciliation.unresolved;
                let mut safety_error = reconciliation.error;
                let mut fill_hash = None;
                let mut fill_block = u64::MAX;
                let mut fill_count = 0usize;
                let mut tokens = U256::ZERO;
                let mut actual = U256::ZERO;
                let gas_spent = reconciliation
                    .finals
                    .iter()
                    .fold(U256::ZERO, |total, state| {
                        total.saturating_add(state.gas_cost())
                    });
                for final_state in reconciliation.finals {
                    if let TxFinal::Confirmed {
                        hash,
                        block_number,
                        logs,
                        ..
                    } = final_state
                    {
                        let got = parse_curve_buy_tokens(&logs, ev.curve, me);
                        if got.is_zero() {
                            continue;
                        }
                        let spent = rules_now
                            .eth_per_buy
                            .saturating_sub(parse_curve_refund(&logs, ev.curve, me));
                        if spent.is_zero() {
                            safety_error.get_or_insert_with(|| {
                                format!("entry {hash:#x} emitted tokens but zero net ETH")
                            });
                            continue;
                        }
                        fill_count += 1;
                        fill_hash.get_or_insert(hash);
                        fill_block = fill_block.min(block_number);
                        tokens = tokens.saturating_add(got);
                        actual = actual.saturating_add(spent);
                    }
                }
                match rpc.get_transaction_count(Lane::Hot, me, true).await {
                    Ok(nonce) => wallet.set_nonce(nonce),
                    Err(e) => {
                        safety_error
                            .get_or_insert_with(|| format!("nonce refresh after burst: {e}"));
                    }
                }
                if let Some(message) = &safety_error {
                    warn(format!("entry reconciliation: {message}"));
                }
                if fill_count == 0 {
                    outcomes.write(
                        OutcomeKind::Attempt,
                        serde_json::json!({"token":format!("{:#x}",ev.token),"confirmed":false,"entry_second":rules_now.entry_second,"tx":res.hash.map(|hash|format!("{hash:#x}")),"gas_wei":gas_spent.to_string()}),
                    );
                    if unresolved || safety_error.is_some() {
                        halt(&stop, &paused);
                        emit(
                            serde_json::json!({"kind":"entry","token":format!("{:#x}",ev.token),"status":"unresolved","message":safety_error}),
                        );
                        resv.keep_all();
                        return;
                    }
                    if !gas_spent.is_zero() {
                        resv.commit(gas_spent);
                    }
                    if let Err(e) = journal.record(&operation_id, JournalEvent::Failed) {
                        halt(&stop, &paused);
                        emit(
                            serde_json::json!({"kind":"engine_error","message":format!("transaction journal: {e}")}),
                        );
                    }
                    warn(format!("burst: {}", res.message));
                    emit(
                        serde_json::json!({"kind":"entry","token":format!("{:#x}",ev.token),"status":"no_fill","message":res.message}),
                    );
                } else {
                    let hash = fill_hash.expect("nonzero fill count has a hash");
                    if fill_count > 1 {
                        safety_error.get_or_insert_with(|| {
                            format!("{fill_count} burst transactions bought tokens")
                        });
                        unresolved = true;
                    }
                    let first_block = first_block_of_second(&rpc, fill_block, boundary).await;
                    let pos = positions.open_position(
                        ev.token,
                        ev.curve,
                        symbol,
                        name,
                        now_ms() / 1000,
                        Some(hash),
                        false,
                        actual,
                        gas_spent,
                        tokens,
                        crate::chain::CHAIN_ID,
                        Some(me),
                    );
                    resv.commit(actual.saturating_add(gas_spent));
                    if let Err(e) = positions.flush() {
                        halt(&stop, &paused);
                        emit(
                            serde_json::json!({"kind":"engine_error","message":format!("positions flush: {e}")}),
                        );
                        return;
                    }
                    if unresolved || safety_error.is_some() {
                        halt(&stop, &paused);
                    } else if let Err(e) = journal.record(&operation_id, JournalEvent::Applied) {
                        halt(&stop, &paused);
                        emit(
                            serde_json::json!({"kind":"engine_error","message":format!("transaction journal: {e}")}),
                        );
                    }
                    outcomes.write(
                        OutcomeKind::Attempt,
                        serde_json::json!({"token":format!("{:#x}",ev.token),"confirmed":true,"first_block":first_block,"entry_second":rules_now.entry_second,"tx":format!("{hash:#x}"),"gas_wei":gas_spent.to_string()}),
                    );
                    burst.adapt(res.attempt, first_block == Some(true));
                    emit(
                        serde_json::json!({"kind":"fire","token":format!("{:#x}",ev.token),"taxBps":tax,"live":true,"positionId":pos.id,"tx":format!("{hash:#x}"),"ethIn":actual.to_string(),"tokens":tokens.to_string(),"symbol":pos.symbol,"waitedMs":now_ms()-t0,"block":fill_block}),
                    );
                    info(format!(
                        "{}  {} bought {} ETH → {}  {}",
                        muted(crate::fmt::hhmmss(None)),
                        on_neon(" FIRE "),
                        crate::fmt::eth(actual),
                        tokens,
                        neon(&res.message)
                    ));
                    outcomes.write(OutcomeKind::Fire, serde_json::json!({"token": format!("{:#x}", ev.token), "tax": tax, "tx": format!("{hash:#x}"), "tokens": tokens.to_string(), "eth_in": actual.to_string(), "gas_wei": gas_spent.to_string(), "fill": true}));
                    return;
                }
            }
        }
        Err(e) => {
            let _ = journal.record(&operation_id, JournalEvent::Failed);
            halt(&stop, &paused);
            warn(format!("presign: {e}"));
            emit(
                serde_json::json!({"kind":"engine_error","message":format!("burst signing: {e}")}),
            );
        }
    }
    busy.lock().remove(&ev.token);
}

struct BurstReconciliation {
    finals: Vec<TxFinal>,
    unresolved: bool,
    error: Option<String>,
}

async fn reconcile_burst(
    rpc: &Rpc,
    journal: &TxJournal,
    operation_id: &str,
    txs: &[SignedTx],
    outcomes: &[(usize, SendOutcome)],
) -> BurstReconciliation {
    let mut hashes = Vec::new();
    let mut error = None;
    for (index, outcome) in outcomes {
        let Some(tx) = txs.get(*index) else {
            error.get_or_insert_with(|| format!("burst result references missing attempt {index}"));
            continue;
        };
        let event = match outcome {
            SendOutcome::Hash(hash) if *hash != tx.hash => {
                error.get_or_insert_with(|| {
                    format!(
                        "sequencer returned {hash:#x} for signed transaction {:#x}",
                        tx.hash
                    )
                });
                JournalEvent::Unresolved { hash: tx.hash }
            }
            SendOutcome::Hash(_) | SendOutcome::Known => {
                hashes.push(tx.hash);
                JournalEvent::Submitted { hash: tx.hash }
            }
            SendOutcome::Revert { .. } | SendOutcome::Reject { .. } => {
                JournalEvent::Rejected { hash: tx.hash }
            }
            SendOutcome::Error(_) => {
                hashes.push(tx.hash);
                JournalEvent::Unresolved { hash: tx.hash }
            }
        };
        if let Err(e) = journal.record(operation_id, event) {
            error.get_or_insert_with(|| e.to_string());
        }
    }
    let checked = futures_util::future::join_all(
        hashes
            .into_iter()
            .map(|hash| async move { (hash, confirm(rpc, hash, 12_000).await) }),
    )
    .await;
    let mut finals = Vec::new();
    let mut unresolved = false;
    for (hash, result) in checked {
        match result {
            Ok(final_state) => {
                let event = match &final_state {
                    TxFinal::Confirmed {
                        block_number,
                        block_hash,
                        ..
                    } => JournalEvent::Confirmed {
                        hash,
                        block_number: *block_number,
                        block_hash: *block_hash,
                        gas_wei: final_state.gas_cost(),
                    },
                    TxFinal::Reverted {
                        block_number,
                        block_hash,
                        ..
                    } => JournalEvent::Reverted {
                        hash,
                        block_number: *block_number,
                        block_hash: *block_hash,
                        gas_wei: final_state.gas_cost(),
                    },
                    TxFinal::Unresolved { .. } => {
                        unresolved = true;
                        JournalEvent::Unresolved { hash }
                    }
                };
                if let Err(e) = journal.record(operation_id, event) {
                    error.get_or_insert_with(|| e.to_string());
                }
                finals.push(final_state);
            }
            Err(e) => {
                unresolved = true;
                error.get_or_insert_with(|| e.to_string());
            }
        }
    }
    BurstReconciliation {
        finals,
        unresolved,
        error,
    }
}

async fn first_block_of_second(rpc: &Rpc, block_number: u64, second: u64) -> Option<bool> {
    if rpc.block_timestamp(Lane::Hot, block_number).await.ok()? != second {
        return Some(false);
    }
    if block_number == 0 {
        return None;
    }
    Some(
        rpc.block_timestamp(Lane::Hot, block_number - 1)
            .await
            .ok()?
            < second,
    )
}

/// Only http(s) social links are passed to the board — anything else could be
/// a `javascript:`/`data:` URL rendered as an anchor.
fn safe_url(u: &str) -> String {
    let u = u.trim();
    if u.starts_with("https://") || u.starts_with("http://") {
        u.to_string()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_admission_never_exceeds_budget() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateDb::open(dir.path()).unwrap();
        let positions = Arc::new(PositionStore::from_state(state).unwrap());
        let spent = Arc::new(Mutex::new(U256::ZERO));
        let open = Arc::new(AtomicU64::new(0));
        let rules = SnipeRules {
            max_open_positions: 16,
            session_budget_wei: U256::from(25),
            ..Default::default()
        };
        let start = Arc::new(std::sync::Barrier::new(16));
        let attempted = Arc::new(std::sync::Barrier::new(16));
        let admitted = Arc::new(AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let start = start.clone();
                let attempted = attempted.clone();
                let admitted = admitted.clone();
                let spent = spent.clone();
                let open = open.clone();
                let positions = positions.clone();
                let rules = rules.clone();
                scope.spawn(move || {
                    start.wait();
                    let reservation =
                        try_reserve(&spent, &open, &positions, &rules, U256::from(10));
                    if reservation.is_some() {
                        admitted.fetch_add(1, Ordering::SeqCst);
                    }
                    attempted.wait();
                    drop(reservation);
                });
            }
        });
        assert_eq!(admitted.load(Ordering::SeqCst), 2);
        assert_eq!(*spent.lock(), U256::ZERO);
        assert_eq!(open.load(Ordering::SeqCst), 0);
    }
}
