use crate::engine::{decide, live_gate, pick_exit, SnipeRules};
use crate::outcomes::OutcomeLog;
use crate::pons::clock::{now_ms, spawn_clock, ChainClock};
use crate::pons::deployer::{Limiter, SharedIndex};
use crate::pons::enrich::{dev_share_pct, enrich_launch, has_socials, is_contract};
use crate::pons::fingerprint::FarmDetector;
use crate::pons::launches::{watch_launches, LaunchEvent};
use crate::pons::stream::FlowTracker;
use crate::pons::tax::{boundary_instant, snipe_tax_bps};
use crate::rpc::Rpc;
use crate::score::{score_launch, ScoreContext};
use crate::style::{info, muted, neon, on_neon, warn};
use crate::trade::burst::{clock_fire, encode_buy_once, presign_burst, shadow_result, BurstCtl};
use crate::trade::pool::{sell_anywhere, value_now};
use crate::trade::positions::{Exit, PositionStore};
use crate::trade::submitter::Submitter;
use crate::trade::wallet::Wallet;
use crate::Config;
use alloy::primitives::{Address, U256};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub type Emit = Arc<dyn Fn(serde_json::Value) + Send + Sync>;

pub struct EngineHandle {
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    spent: Arc<AtomicU64>,
    pub rules: Arc<Mutex<SnipeRules>>,
    close_tx: mpsc::UnboundedSender<String>,
}

impl EngineHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
    pub fn spent(&self) -> U256 {
        U256::from(self.spent.load(Ordering::SeqCst))
    }
    pub fn request_close(&self, id: String) {
        let _ = self.close_tx.send(id);
    }
}

pub fn start_engine(rpc: Arc<Rpc>, cfg: Config, rules: SnipeRules, live: bool, start_paused: bool, emit: Emit) -> EngineHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let paused = Arc::new(AtomicBool::new(start_paused));
    let spent = Arc::new(AtomicU64::new(0));
    let rules = Arc::new(Mutex::new(rules));
    let (close_tx, close_rx) = mpsc::unbounded_channel::<String>();
    let handle = EngineHandle {
        stop: stop.clone(),
        paused: paused.clone(),
        spent: spent.clone(),
        rules: rules.clone(),
        close_tx,
    };

    tokio::spawn(run_loop(rpc, cfg, rules, live, stop, paused, spent, emit, close_rx));
    handle
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    rpc: Arc<Rpc>,
    cfg: Config,
    rules: Arc<Mutex<SnipeRules>>,
    live: bool,
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    spent: Arc<AtomicU64>,
    emit: Emit,
    mut close_rx: mpsc::UnboundedReceiver<String>,
) {
    let wallet = match Wallet::from_env() {
        Ok(w) => w.map(Arc::new),
        Err(e) => {
            warn(format!("wallet: {e}"));
            None
        }
    };
    if live && wallet.is_none() {
        warn("--live needs PRIVATE_KEY in .env");
        return;
    }
    let recipient = wallet.as_ref().map(|w| w.address()).unwrap_or(crate::chain::DEAD);
    let positions = Arc::new(PositionStore::open("data"));
    let outcomes = Arc::new(OutcomeLog::open("data"));
    let clock = Arc::new(ChainClock::default());
    spawn_clock(rpc.clone(), clock.clone(), cfg.rpc_ws.first().cloned()).await;
    let submitter = Submitter::new(&cfg).ok().map(Arc::new);
    if let Some(s) = &submitter {
        let _ = s.resolve_and_pin().await;
        s.spawn_refresh();
    }
    let burst = Arc::new(BurstCtl::from_env());
    let index: SharedIndex = Arc::new(tokio::sync::Mutex::new(crate::pons::deployer::DeployerIndex::default()));
    {
        let rpc = rpc.clone();
        let index = index.clone();
        tokio::spawn(async move {
            if let Ok(evs) = crate::pons::launches::recent_launches(&rpc, 1_728_000, None, None).await {
                let mut idx = index.lock().await;
                for ev in &evs {
                    idx.note(ev);
                }
                idx.mark_ready();
            }
        });
    }
    let (tx, mut rx) = mpsc::unbounded_channel::<LaunchEvent>();
    let watch = watch_launches(rpc.clone(), cfg.clone(), tx);
    let farms = Arc::new(Mutex::new(FarmDetector::default()));
    let flow = Arc::new(Mutex::new(FlowTracker::default()));
    let busy = Arc::new(Mutex::new(HashSet::<Address>::new()));
    let managing = Arc::new(AtomicBool::new(false));

    {
        let positions = positions.clone();
        let rpc = rpc.clone();
        let wallet = wallet.clone();
        let rules = rules.clone();
        let emit = emit.clone();
        let flow = flow.clone();
        let managing = managing.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(1000));
            let mut n = 0u64;
            loop {
                tick.tick().await;
                n += 1;
                if managing.swap(true, Ordering::SeqCst) {
                    continue;
                }
                for pos in positions.open_positions() {
                    let tokens: U256 = pos.tokens.parse().unwrap_or(U256::ZERO);
                    let age = now_ms() / 1000 - pos.opened_at;
                    if age >= 60 && n % 30 != 0 {
                        continue;
                    }
                    if let Ok(Some((eth, venue))) = value_now(&rpc, pos.token, tokens).await {
                        let peak: U256 = pos.peak_eth.parse().unwrap_or(eth);
                        let peak = peak.max(eth);
                        let _ = positions.update(&pos.id, |p| {
                            p.last_eth = eth.to_string();
                            p.last_at = now_ms() / 1000;
                            p.peak_eth = peak.to_string();
                        });
                        let snap = flow.lock().snapshot(pos.curve);
                        let rules = rules.lock().clone();
                        let graduated = venue == "pool";
                        if let Some(act) = pick_exit(&pos, eth, &rules, Some(&snap), now_ms() / 1000, graduated, if graduated { 1.0 } else { 0.0 }) {
                            let qty = tokens * U256::from(act.fraction_bps) / U256::from(10_000u64);
                            if let Ok(res) = sell_anywhere(&rpc, wallet.as_deref(), pos.token, qty, rules.slippage_bps, !live).await {
                                let out = res.eth_out.unwrap_or(res.eth_quoted);
                                let _ = positions.update(&pos.id, |p| {
                                    if act.fraction_bps >= 10_000 {
                                        p.status = "closed".into();
                                    }
                                    p.exits.push(Exit {
                                        at: now_ms() / 1000,
                                        tokens: qty.to_string(),
                                        eth_out: out.to_string(),
                                        reason: act.reason.clone(),
                                        tx: res.hash.map(|h| format!("{h:#x}")),
                                        dry_run: !live,
                                    });
                                });
                                emit(serde_json::json!({"kind":"exit","positionId":pos.id,"symbol":pos.symbol,"ethOut":out.to_string(),"reason":act.reason,"venue":res.venue,"pnlPct":0.0}));
                            }
                        } else {
                            emit(serde_json::json!({"kind":"mark","positionId":pos.id,"symbol":pos.symbol,"valueEth":eth.to_string(),"venue":venue,"pnlPct":0.0}));
                        }
                    }
                }
                positions.flush_if_due();
                managing.store(false, Ordering::SeqCst);
            }
        });
    }

    while !stop.load(Ordering::SeqCst) {
        tokio::select! {
            Some(id) = close_rx.recv() => {
                if let Some(pos) = positions.open_positions().into_iter().find(|p| p.id == id) {
                    let tokens: U256 = pos.tokens.parse().unwrap_or(U256::ZERO);
                    let slip = rules.lock().slippage_bps;
                    if let Ok(res) = sell_anywhere(&rpc, wallet.as_deref(), pos.token, tokens, slip, !live).await {
                        let out = res.eth_out.unwrap_or(res.eth_quoted);
                        let _ = positions.update(&pos.id, |p| {
                            p.status = "closed".into();
                            p.exits.push(Exit { at: now_ms()/1000, tokens: tokens.to_string(), eth_out: out.to_string(), reason: "closed by hand".into(), tx: res.hash.map(|h| format!("{h:#x}")), dry_run: !live });
                        });
                        emit(serde_json::json!({"kind":"exit","positionId":pos.id,"symbol":pos.symbol,"ethOut":out.to_string(),"reason":"closed by hand","venue":res.venue,"pnlPct":0.0}));
                    }
                }
            }
            Some(ev) = rx.recv() => {
                let rpc = rpc.clone();
                let rules = rules.clone();
                let paused = paused.clone();
                let spent = spent.clone();
                let emit = emit.clone();
                let index = index.clone();
                let farms = farms.clone();
                let flow = flow.clone();
                let busy = busy.clone();
                let positions = positions.clone();
                let outcomes = outcomes.clone();
                let clock = clock.clone();
                let burst = burst.clone();
                let submitter = submitter.clone();
                let wallet = wallet.clone();
                let helper = cfg.helper;
                tokio::spawn(async move {
                    handle_launch(rpc, ev, recipient, rules, paused, spent, emit, index, farms, flow, busy, positions, outcomes, clock, burst, submitter, wallet, helper, live).await;
                });
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
        }
    }
    watch.stop();
}

#[allow(clippy::too_many_arguments)]
async fn handle_launch(
    rpc: Arc<Rpc>,
    ev: LaunchEvent,
    recipient: Address,
    rules: Arc<Mutex<SnipeRules>>,
    paused: Arc<AtomicBool>,
    spent: Arc<AtomicU64>,
    emit: Emit,
    index: SharedIndex,
    farms: Arc<Mutex<FarmDetector>>,
    flow: Arc<Mutex<FlowTracker>>,
    busy: Arc<Mutex<HashSet<Address>>>,
    positions: Arc<PositionStore>,
    outcomes: Arc<OutcomeLog>,
    clock: Arc<ChainClock>,
    burst: Arc<BurstCtl>,
    submitter: Option<Arc<Submitter>>,
    wallet: Option<Arc<Wallet>>,
    helper: Option<Address>,
    live: bool,
) {
    if !busy.lock().insert(ev.token) {
        return;
    }
    let t0 = now_ms();
    index.lock().await.note(&ev);
    let limiter = Limiter::new(3);
    let intel = limiter.run(|| enrich_launch(&rpc, ev.clone(), recipient)).await;
    let dq = index.lock().await.quick(ev.deployer, ev.block_number);
    let twins = farms.lock().note(&intel, now_ms()).0;
    let mut ctx = ScoreContext { deployer: dq, farm_twins: twins, ..Default::default() };
    if let Some(rec) = &intel.record {
        ctx.fee_recipient_is_contract = is_contract(&rpc, rec.creator_fee_recipient).await.ok();
    }
    let score = score_launch(&intel, &ctx);
    let rules_now = rules.lock().clone();
    let open = positions.open_positions().len();
    let mut d = decide(&intel, &score, &rules_now, open, twins, U256::from(spent.load(Ordering::SeqCst)));
    if d.fire && paused.load(Ordering::SeqCst) {
        d.why.push(if live { "not armed".into() } else { "demo not started".into() });
        d.fire = false;
    }
    let soc = has_socials(intel.meta.as_ref());
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
            "socials": {"x": if soc.twitter { intel.meta.as_ref().and_then(|m| Some(m.socials.twitter.clone())).unwrap_or_default() } else { String::new() }},
            "deployer": format!("{:#x}", ev.deployer),
            "deployerPrior": dq.map(|d| d.0), "deployerGraduated": dq.map(|d| d.1),
            "exempt": intel.tx.as_ref().map(|t| t.exemptions.iter().map(|a| format!("{a:#x}")).collect::<Vec<_>>()),
            "openingTaxBps": intel.curve.as_ref().map(|c| c.opening_tax_bps.to_string()),
            "block": ev.block_number.to_string(), "tx": format!("{:#x}", ev.tx_hash), "errors": intel.errors
        }
    }));
    outcomes.write("launch_seen", serde_json::json!({"token": format!("{:#x}", ev.token), "score": score.total}));
    if !d.fire {
        busy.lock().remove(&ev.token);
        return;
    }
    if let Some(c) = &intel.curve {
        flow.lock().watch(ev.curve, c.launched_at, intel.tx.as_ref().map(|t| t.exemptions.clone()).unwrap_or_default());
    }
    let launched_at = intel.curve.as_ref().map(|c| c.launched_at).or_else(|| intel.tx.as_ref().map(|t| t.timestamp)).unwrap_or(0);
    let start_bps = intel.curve.as_ref().and_then(|c| c.snipe_tax_start_bps.try_into().ok()).unwrap_or(9900);
    let window = intel.curve.as_ref().and_then(|c| c.snipe_tax_seconds.try_into().ok()).unwrap_or(3);
    clock.sleep_until_unix(boundary_instant(launched_at, rules_now.entry_second).saturating_sub(1)).await;
    let snap = flow.lock().snapshot(ev.curve);
    let g = live_gate(&rules_now, &snap);
    if !g.fire {
        info(format!("{}  hold  {}", muted(crate::fmt::hhmmss(None)), g.why.join("; ")));
        emit(serde_json::json!({"kind":"hold","token":format!("{:#x}",ev.token),"why":g.why}));
        busy.lock().remove(&ev.token);
        return;
    }
    let tax = snipe_tax_bps(start_bps, window, rules_now.entry_second);
    let quoted = intel.curve.as_ref().map(|c| crate::pons::curve::quote_buy(c, rules_now.eth_per_buy).tokens_out).unwrap_or(U256::ZERO);
    if !live {
        let sh = shadow_result(launched_at, rules_now.entry_second, 0, Some(0));
        let pos = positions.open_position(ev.token, ev.curve, intel.meta.as_ref().map(|m| m.symbol.clone()).unwrap_or_else(|| "?".into()), intel.meta.as_ref().map(|m| m.name.clone()).unwrap_or_else(|| "?".into()), now_ms() / 1000, None, true, rules_now.eth_per_buy, quoted);
        spent.fetch_add(u64_sat(rules_now.eth_per_buy), Ordering::SeqCst);
        info(format!("{}  {} would buy {} ETH at tax {}  {}", muted(crate::fmt::hhmmss(None)), on_neon(" FIRE "), crate::fmt::eth(rules_now.eth_per_buy), crate::fmt::bps(tax), muted(&sh.message)));
        emit(serde_json::json!({"kind":"fire","token":format!("{:#x}",ev.token),"taxBps":tax,"waitedMs":now_ms()-t0,"live":false,"positionId":pos.id,"ethIn":rules_now.eth_per_buy.to_string(),"tokens":quoted.to_string(),"symbol":pos.symbol}));
        outcomes.write("fire", serde_json::json!({"token": format!("{:#x}", ev.token), "tax": tax, "first_block": true, "fill": true}));
        busy.lock().remove(&ev.token);
        return;
    }
    let (Some(helper), Some(wallet), Some(sub)) = (helper, wallet, submitter) else {
        warn("live burst needs HELPER_ADDRESS, PRIVATE_KEY and sequencer");
        busy.lock().remove(&ev.token);
        return;
    };
    let min_out = intel.curve.as_ref().map(|c| crate::pons::curve::min_out_as_last_in_block(c, rules_now.eth_per_buy, U256::from(100_000_000_000_000_000u128), rules_now.slippage_bps)).unwrap_or(U256::ZERO);
    let data = encode_buy_once(ev.curve, ev.token, recipient, U256::from(rules_now.max_opening_tax_bps), min_out, U256::from(4_200_000_000_000_000_000u128));
    if let Ok(nonce) = rpc.get_transaction_count(crate::rpc::Lane::Hot, wallet.address(), true).await {
        wallet.set_nonce(nonce);
    }
    match presign_burst(&wallet, &clock, helper, data, rules_now.eth_per_buy, burst.max) {
        Ok(txs) => {
            let res = clock_fire(&clock, launched_at, rules_now.entry_second, burst.lead_ms(), &sub, &txs).await;
            outcomes.write("attempt", serde_json::json!({"token": format!("{:#x}", ev.token), "class": format!("{:?}", res.class), "attempt": res.attempt, "fill": res.hash.is_some()}));
            if res.hash.is_some() {
                let pos = positions.open_position(ev.token, ev.curve, intel.meta.as_ref().map(|m| m.symbol.clone()).unwrap_or_else(|| "?".into()), intel.meta.as_ref().map(|m| m.name.clone()).unwrap_or_else(|| "?".into()), now_ms() / 1000, res.hash, false, rules_now.eth_per_buy, quoted);
                spent.fetch_add(u64_sat(rules_now.eth_per_buy), Ordering::SeqCst);
                emit(serde_json::json!({"kind":"fire","token":format!("{:#x}",ev.token),"taxBps":tax,"live":true,"positionId":pos.id,"tx":res.hash.map(|h| format!("{h:#x}")),"ethIn":rules_now.eth_per_buy.to_string(),"tokens":quoted.to_string(),"symbol":pos.symbol,"waitedMs":now_ms()-t0}));
                info(format!("{}  {} bought {} ETH  {}", muted(crate::fmt::hhmmss(None)), on_neon(" FIRE "), crate::fmt::eth(rules_now.eth_per_buy), neon(&res.message)));
            } else {
                warn(format!("burst: {}", res.message));
            }
        }
        Err(e) => warn(format!("presign: {e}")),
    }
    busy.lock().remove(&ev.token);
}

fn u64_sat(v: U256) -> u64 {
    v.try_into().unwrap_or(u64::MAX)
}
