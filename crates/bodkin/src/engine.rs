use crate::config::{env_num, env_u64, parse_ether, parse_exit_ladder};
use crate::fmt::{bps, eth};
use crate::pons::curve::progress;
use crate::pons::enrich::{dev_share_pct, has_socials, LaunchIntel};
use crate::pons::stream::FlowSnapshot;
use crate::score::Score;
use crate::trade::positions::{ExitAction, ExitLadder, ExitRules, Position};
use alloy::primitives::U256;
use regex::Regex;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct SnipeRules {
    pub eth_per_buy: U256,
    pub slippage_bps: u64,
    pub max_opening_tax_bps: u64,
    pub min_score: i32,
    pub max_dev_share_pct: f64,
    pub min_dev_share_pct: f64,
    pub max_creator_tax_bps: u16,
    pub require_socials: bool,
    pub max_exempt_wallets: usize,
    pub eth_pairs_only: bool,
    pub keyword: Option<Regex>,
    pub deployers: HashSet<String>,
    pub max_open_positions: usize,
    pub max_farm_twins: u32,
    pub session_budget_wei: U256,
    pub exits: ExitRules,
    pub max_wait_ms: u64,
    pub entry_second: u64,
    pub min_taxed_buyers_s1: u32,
    pub max_exempt_buys_s0: u32,
    pub abort_if_insider_sold: bool,
    pub ladder: ExitLadder,
    pub stale_sec: u64,
    pub stale_min_progress: f64,
}

impl Default for SnipeRules {
    fn default() -> Self {
        rules_from_env()
    }
}

pub fn rules_from_env() -> SnipeRules {
    let eth_per = parse_ether(&format!("{}", env_num("SNIPE_ETH", 0.01))).unwrap_or(U256::from(10_000_000_000_000_000u64));
    let budget = parse_ether(&format!("{}", env_num("SNIPE_BUDGET_ETH", 0.05))).unwrap_or(eth_per * U256::from(5u64));
    let ladder_raw = crate::config::env_str("EXIT_LADDER").unwrap_or_else(|| "34@100,33@300".into());
    SnipeRules {
        eth_per_buy: eth_per,
        slippage_bps: env_u64("SNIPE_SLIPPAGE_BPS", 300),
        max_opening_tax_bps: env_u64("SNIPE_MAX_TAX_BPS", 300),
        min_score: 60,
        max_dev_share_pct: 8.0,
        min_dev_share_pct: 0.0,
        max_creator_tax_bps: 300,
        require_socials: true,
        max_exempt_wallets: 2,
        eth_pairs_only: true,
        keyword: None,
        deployers: HashSet::new(),
        max_open_positions: 3,
        max_farm_twins: 1,
        session_budget_wei: budget,
        exits: ExitRules {
            take_profit_pct: env_num("TAKE_PROFIT_PCT", 80.0),
            stop_loss_pct: env_num("STOP_LOSS_PCT", 35.0),
            trailing_pct: env_num("TRAILING_PCT", 25.0),
            max_hold_min: env_num("MAX_HOLD_MIN", 45.0),
        },
        max_wait_ms: 12_000,
        entry_second: env_u64("ENTRY_SECOND", 2),
        min_taxed_buyers_s1: env_u64("MIN_TAXED_BUYERS_S1", 0) as u32,
        max_exempt_buys_s0: env_u64("MAX_EXEMPT_BUYS_S0", 32) as u32,
        abort_if_insider_sold: crate::config::env_bool("ABORT_IF_INSIDER_SOLD", true),
        ladder: ExitLadder { rungs: parse_exit_ladder(&ladder_raw) },
        stale_sec: env_u64("STALE_SEC", 90),
        stale_min_progress: env_num("STALE_MIN_PROGRESS", 0.02),
    }
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub fire: bool,
    pub why: Vec<String>,
}

/// Pure filter. A launch the RPC would not let us read is not a rule failure.
/// Missing launch tx is a refuse (do not fire on a null tx).
pub fn decide(intel: &LaunchIntel, score: &Score, rules: &SnipeRules, open_count: usize, farm_twins: u32, spent_wei: U256) -> Decision {
    if intel.meta.is_none() && intel.record.is_none() && intel.curve.is_none() {
        return Decision {
            fire: false,
            why: vec![format!("unreadable: {}", intel.errors.first().map(String::as_str).unwrap_or("no data"))],
        };
    }
    if intel.tx.is_none() {
        return Decision { fire: false, why: vec!["unreadable: launch tx missing".into()] };
    }
    let mut why = Vec::new();
    if open_count >= rules.max_open_positions {
        why.push(format!("open positions {open_count} ≥ {}", rules.max_open_positions));
    }
    if spent_wei + rules.eth_per_buy > rules.session_budget_wei {
        why.push(format!("session budget {} ETH reached ({} spent)", eth(rules.session_budget_wei), eth(spent_wei)));
    }
    if farm_twins > rules.max_farm_twins {
        why.push(format!("launch farm: {farm_twins} twins in 30 min > {}", rules.max_farm_twins));
    }
    if rules.eth_pairs_only && intel.pair.symbol != "ETH" {
        why.push(format!("pair is {}, not ETH", intel.pair.symbol));
    }
    if score.total < rules.min_score {
        why.push(format!("score {} < {}", score.total, rules.min_score));
    }
    let dev = dev_share_pct(intel.tx.as_ref());
    if dev > rules.max_dev_share_pct {
        why.push(format!("dev share {dev:.2}% > {}%", rules.max_dev_share_pct));
    }
    if dev < rules.min_dev_share_pct {
        why.push(format!("dev share {dev:.2}% < {}%", rules.min_dev_share_pct));
    }
    if let Some(rec) = &intel.record {
        if rec.creator_tax_bps > rules.max_creator_tax_bps {
            why.push(format!(
                "creator tax {} > {}",
                bps(rec.creator_tax_bps as u64),
                bps(rules.max_creator_tax_bps as u64)
            ));
        }
    }
    if rules.require_socials && !has_socials(intel.meta.as_ref()).any {
        why.push("no socials".into());
    }
    if let Some(tx) = &intel.tx {
        if tx.exemptions.len() > rules.max_exempt_wallets {
            why.push(format!("{} exempt wallets > {}", tx.exemptions.len(), rules.max_exempt_wallets));
        }
    }
    if let Some(re) = &rules.keyword {
        let hay = format!(
            "{} {} {}",
            intel.meta.as_ref().map(|m| m.name.as_str()).unwrap_or(""),
            intel.meta.as_ref().map(|m| m.symbol.as_str()).unwrap_or(""),
            intel.meta.as_ref().map(|m| m.description.as_str()).unwrap_or(""),
        );
        if !re.is_match(&hay) {
            why.push(format!("keyword {re} not found"));
        }
    }
    if !rules.deployers.is_empty() && !rules.deployers.contains(&format!("{:#x}", intel.ev.deployer)) {
        why.push("deployer not on the allow-list".into());
    }
    match &intel.curve {
        Some(c) if !c.graduated && !c.ready_to_graduate => {}
        _ => why.push("curve not open".into()),
    }
    Decision { fire: why.is_empty(), why }
}

/// Live early-flow gate, evaluated at `boundary − lead`. Separate from `decide` so score/rules stay readable.
pub fn live_gate(rules: &SnipeRules, flow: &FlowSnapshot) -> Decision {
    let mut why = Vec::new();
    if flow.taxed_buyers_s1 < rules.min_taxed_buyers_s1 {
        why.push(format!("taxed buyers in s1 {} < {}", flow.taxed_buyers_s1, rules.min_taxed_buyers_s1));
    }
    if flow.exempt_buys_s0 > rules.max_exempt_buys_s0 {
        why.push(format!("exempt buys in s0 {} > {}", flow.exempt_buys_s0, rules.max_exempt_buys_s0));
    }
    if rules.abort_if_insider_sold && flow.insider_sold {
        why.push("insider sold before entry".into());
    }
    Decision { fire: why.is_empty(), why }
}

/// On-curve: ladder / stale / insider. After graduation: SL / trail / max-hold (the Node exitReason set).
pub fn pick_exit(pos: &Position, value_eth: U256, rules: &SnipeRules, flow: Option<&FlowSnapshot>, now_sec: u64, graduated: bool, curve_progress: f64) -> Option<ExitAction> {
    if !graduated {
        if let Some(f) = flow {
            if f.insider_sold {
                return Some(ExitAction { fraction_bps: 10_000, reason: "insider sold".into() });
            }
        }
        if let Some(a) = rules.ladder.hit(pos, value_eth) {
            return Some(a);
        }
        if now_sec.saturating_sub(pos.opened_at) >= rules.stale_sec && curve_progress < rules.stale_min_progress {
            return Some(ExitAction { fraction_bps: 10_000, reason: format!("stale: <{:.0}% progress after {}s", rules.stale_min_progress * 100.0, rules.stale_sec) });
        }
        if curve_progress >= 0.90 || flow.is_some_and(|f| f.ready_to_graduate) {
            return Some(ExitAction { fraction_bps: 10_000, reason: format!("curve {:.0}% / ready to graduate", curve_progress * 100.0) });
        }
        return None;
    }
    crate::trade::positions::exit_reason(pos, value_eth, &rules.exits, now_sec)
        .map(|reason| ExitAction { fraction_bps: 10_000, reason })
}

pub fn curve_progress_of(intel: &LaunchIntel) -> f64 {
    intel.curve.as_ref().map(progress).unwrap_or(0.0)
}
