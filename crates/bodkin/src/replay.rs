use crate::engine::{decide, SnipeRules};
use crate::pons::curve::{progress, quote_buy, CurveState};
use crate::pons::enrich::LaunchIntel;
use crate::pons::tax::snipe_tax_bps;
use crate::score::{score_launch, ScoreContext};
use alloy::primitives::U256;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayLaunch {
    pub token: String,
    pub launched_at: u64,
    pub start_bps: u64,
    pub window: u64,
    pub taxed_buyers_s1: u32,
    pub exempt_buys_s0: u32,
    pub graduated: bool,
    pub peak_progress: f64,
    pub intel: Option<ReplayIntel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayIntel {
    pub score_total: i32,
    pub fire: bool,
}

#[derive(Debug, Clone)]
pub struct ReplayAlt {
    pub entry_second: u64,
    pub min_taxed_s1: u32,
}

#[derive(Debug, Default)]
pub struct ReplayReport {
    pub n: u64,
    pub would_fire: u64,
    pub would_skip_gate: u64,
    pub graduated_of_fires: u64,
}

/// Sampled replay: TokenLaunched + first-60-block curve path vs alternative rules / entry seconds.
pub fn replay(launches: &[ReplayLaunch], rules: &SnipeRules, alt: &ReplayAlt) -> ReplayReport {
    let mut r = ReplayReport::default();
    for l in launches {
        r.n += 1;
        let tax = snipe_tax_bps(l.start_bps, l.window, alt.entry_second);
        let gate_ok = l.taxed_buyers_s1 >= alt.min_taxed_s1 && tax <= rules.max_opening_tax_bps;
        if !gate_ok {
            r.would_skip_gate += 1;
            continue;
        }
        r.would_fire += 1;
        if l.graduated {
            r.graduated_of_fires += 1;
        }
    }
    r
}

pub fn print_report(r: &ReplayReport, label: &str) {
    let rate = if r.would_fire == 0 { 0.0 } else { r.graduated_of_fires as f64 / r.would_fire as f64 * 100.0 };
    println!("{label}: n={} fire={} skip_gate={} graduated_of_fires={} ({:.1}%)", r.n, r.would_fire, r.would_skip_gate, r.graduated_of_fires, rate);
}

/// Reserve path: apply a sequence of quote-in buys to a curve copy.
pub fn reserve_path(start: &CurveState, buys: &[U256]) -> CurveState {
    let mut s = start.clone();
    for q in buys {
        let b = quote_buy(&s, *q);
        s.quote_reserve += b.spent;
        s.token_reserve = s.token_reserve.saturating_sub(b.tokens_out);
        s.real_quote_reserve += b.spent;
        s.sellable_tokens = s.sellable_tokens.saturating_sub(b.tokens_out);
    }
    s
}

pub fn would_decide(intel: &LaunchIntel, rules: &SnipeRules, farm: u32, spent: U256, open: usize) -> bool {
    let score = score_launch(intel, &ScoreContext::default());
    decide(intel, &score, rules, open, farm, spent).fire
}

pub fn path_progress(s: &CurveState) -> f64 {
    progress(s)
}
