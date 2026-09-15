use crate::config::{
    env_num, env_u64, parse_ether, parse_exit_ladder, strict_env_bool, strict_env_num,
    strict_env_u64,
};
use crate::fmt::{bps, eth};
use crate::pons::enrich::{LaunchIntel, dev_share_pct, has_socials};
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

fn env_ether(key: &str, default_eth: &str) -> U256 {
    match crate::config::env_str(key) {
        Some(v) => match parse_ether(&v) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("warning: {key}={v:?} ignored: {e}");
                parse_ether(default_eth).unwrap_or(U256::ZERO)
            }
        },
        None => parse_ether(default_eth).unwrap_or(U256::ZERO),
    }
}

pub fn rules_from_env() -> SnipeRules {
    let eth_per = env_ether("SNIPE_ETH", "0.01");
    let budget = env_ether("SNIPE_BUDGET_ETH", "0.05");
    let ladder_raw =
        crate::config::env_str("EXIT_LADDER").unwrap_or_else(|| "34@100,33@300".into());
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
        ladder: ExitLadder {
            rungs: parse_exit_ladder(&ladder_raw),
        },
        stale_sec: env_u64("STALE_SEC", 90),
        stale_min_progress: env_num("STALE_MIN_PROGRESS", 0.02),
    }
}

pub fn try_rules_from_env() -> anyhow::Result<SnipeRules> {
    let mut rules = rules_from_env();
    if let Some(value) = crate::config::env_str("SNIPE_ETH") {
        rules.eth_per_buy = parse_ether(&value)?;
    }
    if let Some(value) = crate::config::env_str("SNIPE_BUDGET_ETH") {
        rules.session_budget_wei = parse_ether(&value)?;
    }
    rules.slippage_bps = strict_env_u64("SNIPE_SLIPPAGE_BPS", 300)?;
    rules.max_opening_tax_bps = strict_env_u64("SNIPE_MAX_TAX_BPS", 300)?;
    rules.exits.take_profit_pct = strict_env_num("TAKE_PROFIT_PCT", 80.0)?;
    rules.exits.stop_loss_pct = strict_env_num("STOP_LOSS_PCT", 35.0)?;
    rules.exits.trailing_pct = strict_env_num("TRAILING_PCT", 25.0)?;
    rules.exits.max_hold_min = strict_env_num("MAX_HOLD_MIN", 45.0)?;
    rules.entry_second = strict_env_u64("ENTRY_SECOND", 2)?;
    rules.min_taxed_buyers_s1 = strict_env_u64("MIN_TAXED_BUYERS_S1", 0)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("MIN_TAXED_BUYERS_S1 does not fit u32"))?;
    rules.max_exempt_buys_s0 = strict_env_u64("MAX_EXEMPT_BUYS_S0", 32)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("MAX_EXEMPT_BUYS_S0 does not fit u32"))?;
    rules.abort_if_insider_sold = strict_env_bool("ABORT_IF_INSIDER_SOLD", true)?;
    rules.stale_sec = strict_env_u64("STALE_SEC", 90)?;
    rules.stale_min_progress = strict_env_num("STALE_MIN_PROGRESS", 0.02)?;
    if let Some(raw) = crate::config::env_str("EXIT_LADDER") {
        let rungs = parse_exit_ladder(&raw);
        let expected = raw
            .split(',')
            .filter(|part| !part.trim().is_empty())
            .count();
        anyhow::ensure!(
            rungs.len() == expected,
            "EXIT_LADDER contains malformed or out-of-range entries"
        );
        rules.ladder = ExitLadder { rungs };
    }
    rules.validate()?;
    Ok(rules)
}

impl SnipeRules {
    /// Hard validation for the CLI boundary — bad numbers refuse to arm rather
    /// than silently degrading into wrong behavior (e.g. slippage ≥ 100%
    /// underflowing `min_out`).
    pub fn validate(&self) -> anyhow::Result<()> {
        use crate::chain::BPS;
        if self.eth_per_buy.is_zero() {
            anyhow::bail!("entry amount is zero (SNIPE_ETH / --eth)");
        }
        if self.slippage_bps >= BPS {
            anyhow::bail!("slippage {} bps is not < 10000", self.slippage_bps);
        }
        if self.max_opening_tax_bps > BPS {
            anyhow::bail!(
                "max opening tax {} bps is not ≤ 10000",
                self.max_opening_tax_bps
            );
        }
        if self.session_budget_wei < self.eth_per_buy {
            anyhow::bail!("session budget below one entry (SNIPE_BUDGET_ETH)");
        }
        if self.entry_second == 0 {
            anyhow::bail!("ENTRY_SECOND must be ≥ 1 (first block of the second)");
        }
        if self.max_open_positions == 0 {
            anyhow::bail!("max open positions is 0 — nothing could ever fire");
        }
        if self.stale_sec == 0 {
            anyhow::bail!("STALE_SEC must be ≥ 1");
        }
        for (frac, at) in &self.ladder.rungs {
            if *frac == 0 || *frac > 100 {
                anyhow::bail!("EXIT_LADDER fraction {frac}% outside 1..=100");
            }
            let _ = at;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub fire: bool,
    pub why: Vec<String>,
}

/// Pure filter. A launch the RPC would not let us read is not a rule failure.
/// Missing launch tx is a refuse (do not fire on a null tx).
pub fn decide(
    intel: &LaunchIntel,
    score: &Score,
    rules: &SnipeRules,
    open_count: usize,
    farm_twins: u32,
    spent_wei: U256,
) -> Decision {
    if intel.meta.is_none() && intel.record.is_none() && intel.curve.is_none() {
        return Decision {
            fire: false,
            why: vec![format!(
                "unreadable: {}",
                intel
                    .errors
                    .first()
                    .map(String::as_str)
                    .unwrap_or("no data")
            )],
        };
    }
    if intel.tx.is_none() {
        return Decision {
            fire: false,
            why: vec!["unreadable: launch tx missing".into()],
        };
    }
    let mut why = Vec::new();
    // Without the factory record the creator-tax rule cannot run — refuse
    // rather than letting an unknown tax slide.
    if intel.record.is_none() {
        why.push("unreadable: factory record missing".into());
    }
    if open_count >= rules.max_open_positions {
        why.push(format!(
            "open positions {open_count} ≥ {}",
            rules.max_open_positions
        ));
    }
    if spent_wei + rules.eth_per_buy > rules.session_budget_wei {
        why.push(format!(
            "session budget {} ETH reached ({} spent)",
            eth(rules.session_budget_wei),
            eth(spent_wei)
        ));
    }
    if farm_twins > rules.max_farm_twins {
        why.push(format!(
            "launch farm: {farm_twins} twins in 30 min > {}",
            rules.max_farm_twins
        ));
    }
    if rules.eth_pairs_only && !intel.pair.address.is_zero() {
        why.push(format!("pair is {}, not ETH", intel.pair.symbol));
    }
    if score.total < rules.min_score {
        why.push(format!("score {} < {}", score.total, rules.min_score));
    }
    let dev = dev_share_pct(intel.tx.as_ref());
    if dev > rules.max_dev_share_pct {
        why.push(format!(
            "dev share {dev:.2}% > {}%",
            rules.max_dev_share_pct
        ));
    }
    if dev < rules.min_dev_share_pct {
        why.push(format!(
            "dev share {dev:.2}% < {}%",
            rules.min_dev_share_pct
        ));
    }
    if let Some(rec) = &intel.record
        && rec.creator_tax_bps > rules.max_creator_tax_bps
    {
        why.push(format!(
            "creator tax {} > {}",
            bps(rec.creator_tax_bps as u64),
            bps(rules.max_creator_tax_bps as u64)
        ));
    }
    if rules.require_socials && !has_socials(intel.meta.as_ref()).any {
        why.push("no socials".into());
    }
    if let Some(tx) = &intel.tx
        && tx.exemptions.len() > rules.max_exempt_wallets
    {
        why.push(format!(
            "{} exempt wallets > {}",
            tx.exemptions.len(),
            rules.max_exempt_wallets
        ));
    }
    if let Some(re) = &rules.keyword {
        let hay = format!(
            "{} {} {}",
            intel.meta.as_ref().map(|m| m.name.as_str()).unwrap_or(""),
            intel.meta.as_ref().map(|m| m.symbol.as_str()).unwrap_or(""),
            intel
                .meta
                .as_ref()
                .map(|m| m.description.as_str())
                .unwrap_or(""),
        );
        if !re.is_match(&hay) {
            why.push(format!("keyword {re} not found"));
        }
    }
    if !rules.deployers.is_empty()
        && !rules
            .deployers
            .contains(&format!("{:#x}", intel.ev.deployer))
    {
        why.push("deployer not on the allow-list".into());
    }
    match &intel.curve {
        Some(c) if !c.graduated && !c.ready_to_graduate => {}
        _ => why.push("curve not open".into()),
    }
    Decision {
        fire: why.is_empty(),
        why,
    }
}

/// Live early-flow gate, evaluated at `boundary − lead`. Separate from `decide` so score/rules stay readable.
pub fn live_gate(rules: &SnipeRules, flow: &FlowSnapshot) -> Decision {
    let mut why = Vec::new();
    if flow.taxed_buyers_s1 < rules.min_taxed_buyers_s1 {
        why.push(format!(
            "taxed buyers in s1 {} < {}",
            flow.taxed_buyers_s1, rules.min_taxed_buyers_s1
        ));
    }
    if flow.exempt_buys_s0 > rules.max_exempt_buys_s0 {
        why.push(format!(
            "exempt buys in s0 {} > {}",
            flow.exempt_buys_s0, rules.max_exempt_buys_s0
        ));
    }
    if rules.abort_if_insider_sold && flow.insider_sold {
        why.push("insider sold before entry".into());
    }
    Decision {
        fire: why.is_empty(),
        why,
    }
}

/// On-curve: ladder / stale / insider. After graduation: SL / trail / max-hold (the Node exitReason set).
pub fn pick_exit(
    pos: &Position,
    value_eth: U256,
    rules: &SnipeRules,
    flow: Option<&FlowSnapshot>,
    now_sec: u64,
    graduated: bool,
    curve_progress: f64,
) -> Option<ExitAction> {
    if !graduated {
        if let Some(f) = flow
            && f.insider_sold
        {
            return Some(ExitAction {
                fraction_bps: 10_000,
                reason: "insider sold".into(),
            });
        }
        if let Some(a) = rules.ladder.hit(pos, value_eth) {
            return Some(a);
        }
        if now_sec.saturating_sub(pos.opened_at) >= rules.stale_sec
            && curve_progress < rules.stale_min_progress
        {
            return Some(ExitAction {
                fraction_bps: 10_000,
                reason: format!(
                    "stale: <{:.0}% progress after {}s",
                    rules.stale_min_progress * 100.0,
                    rules.stale_sec
                ),
            });
        }
        if curve_progress >= 0.90 || flow.is_some_and(|f| f.ready_to_graduate) {
            return Some(ExitAction {
                fraction_bps: 10_000,
                reason: format!("curve {:.0}% / ready to graduate", curve_progress * 100.0),
            });
        }
        return None;
    }
    crate::trade::positions::exit_reason(pos, value_eth, &rules.exits, now_sec).map(|reason| {
        ExitAction {
            fraction_bps: 10_000,
            reason,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pons::curve::CurveState;
    use crate::pons::enrich::{LaunchIntel, LaunchRecord, LaunchTx, PairInfo, Socials, TokenMeta};
    use crate::pons::launches::LaunchEvent;
    use crate::score::{Score, Verdict};
    use alloy::primitives::{Address, B256};

    fn a(n: u64) -> Address {
        Address::from_word(B256::from(U256::from(n)))
    }

    fn intel() -> LaunchIntel {
        LaunchIntel {
            ev: LaunchEvent {
                token: a(1),
                curve: a(2),
                deployer: a(3),
                pair_token: Address::ZERO,
                launch_config_id: U256::ZERO,
                graduation_threshold: U256::ZERO,
                block_number: 1,
                tx_hash: B256::ZERO,
                log_index: 0,
                detected_at_ms: 0,
                source: "unknown",
            },
            meta: Some(TokenMeta {
                name: "test".into(),
                symbol: "T".into(),
                description: String::new(),
                socials: Socials {
                    twitter: "https://x.com/t".into(),
                    ..Default::default()
                },
            }),
            record: Some(LaunchRecord {
                creator_fee_recipient: a(3),
                creator_tax_bps: 100,
                phase: 0,
            }),
            tx: Some(LaunchTx {
                from: a(3),
                dev_buy_wei: U256::ZERO,
                dev_tokens: U256::ZERO,
                exemptions: vec![],
                recipient: a(3),
                timestamp: 100,
            }),
            curve: Some(CurveState::default()),
            pair: PairInfo::eth(),
            errors: vec![],
            fee_recipient_is_contract: None,
            fee_check_ms: 0,
        }
    }

    fn score() -> Score {
        Score {
            total: 100,
            verdict: Verdict::Fire,
            reasons: vec![],
        }
    }

    #[test]
    fn default_rules_validate() {
        rules_from_env().validate().expect("defaults must validate");
    }

    #[test]
    fn validate_rejects_impossible_values() {
        let bad = |f: &dyn Fn(&mut SnipeRules)| {
            let mut r = rules_from_env();
            f(&mut r);
            assert!(r.validate().is_err());
        };
        bad(&|r| r.eth_per_buy = U256::ZERO);
        bad(&|r| r.slippage_bps = 10_000);
        bad(&|r| r.max_opening_tax_bps = 10_001);
        bad(&|r| r.session_budget_wei = U256::ZERO);
        bad(&|r| r.entry_second = 0);
        bad(&|r| r.max_open_positions = 0);
        bad(&|r| r.stale_sec = 0);
    }

    #[test]
    fn decide_refuses_missing_pieces() {
        let rules = rules_from_env();
        let mut i = intel();
        i.tx = None;
        assert!(
            !decide(&i, &score(), &rules, 0, 0, U256::ZERO).fire,
            "missing launch tx must refuse"
        );
        let mut i = intel();
        i.record = None;
        let d = decide(&i, &score(), &rules, 0, 0, U256::ZERO);
        assert!(!d.fire, "missing factory record must refuse");
        assert!(d.why.iter().any(|w| w.contains("factory record")));
    }

    #[test]
    fn decide_pairs_by_address_not_symbol() {
        let rules = rules_from_env();
        let mut i = intel();
        // A token *named* ETH with a real address is not the ETH pair.
        i.pair = PairInfo {
            address: a(77),
            symbol: "ETH".into(),
            decimals: 18,
            usd_per_unit: None,
        };
        let d = decide(&i, &score(), &rules, 0, 0, U256::ZERO);
        assert!(
            !d.fire,
            "pair address non-zero must refuse under eth_pairs_only"
        );
    }

    #[test]
    fn decide_fires_when_clean() {
        let rules = rules_from_env();
        let d = decide(&intel(), &score(), &rules, 0, 0, U256::ZERO);
        assert!(d.fire, "clean intel should fire, got: {:?}", d.why);
    }

    #[test]
    fn live_gate_blocks_insider_sell() {
        let rules = rules_from_env();
        let f = FlowSnapshot {
            insider_sold: true,
            ..Default::default()
        };
        assert!(!live_gate(&rules, &f).fire);
        let mut r = rules_from_env();
        r.min_taxed_buyers_s1 = 2;
        assert!(
            !live_gate(&r, &FlowSnapshot::default()).fire,
            "min taxed buyers unmet"
        );
    }
}
