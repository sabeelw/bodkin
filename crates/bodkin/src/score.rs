use crate::pons::curve::progress;
use crate::pons::enrich::{dev_share_pct, has_socials, CurveActivity, LaunchIntel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Fire,
    Watch,
    Skip,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fire => "FIRE",
            Self::Watch => "WATCH",
            Self::Skip => "SKIP",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Score {
    pub total: i32,
    pub verdict: Verdict,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ScoreContext {
    pub deployer: Option<(u32, u32)>,
    pub activity: Option<CurveActivity>,
    pub age_sec: Option<u64>,
    pub farm_twins: u32,
    pub fire: i32,
    pub watch: i32,
    /// Cached `eth_getCode`. None = not looked up (do not invent a penalty).
    pub fee_recipient_is_contract: Option<bool>,
}

impl ScoreContext {
    fn fire_th(&self) -> i32 {
        if self.fire == 0 { 75 } else { self.fire }
    }
    fn watch_th(&self) -> i32 {
        if self.watch == 0 { 45 } else { self.watch }
    }
}

/// Rule-based, explainable. Starts at 50; clamps to 0..100.
/// Telegram scores the same as a website (plan: Telegram ≥ X/website). Do not double-count
/// socials here and again in `decide` beyond the require-socials hard rule.
pub fn score_launch(intel: &LaunchIntel, ctx: &ScoreContext) -> Score {
    let mut s: i32 = 50;
    let mut r = Vec::new();
    let mut add = |pts: i32, why: &str| {
        s += pts;
        r.push(format!("{} {why}", if pts >= 0 { format!("+{pts}") } else { pts.to_string() }));
    };

    let dev = dev_share_pct(intel.tx.as_ref());
    if intel.tx.is_some() {
        if (dev - 0.0).abs() < f64::EPSILON {
            add(-10, "no dev buy, nothing at stake");
        } else if dev < 1.0 {
            add(0, &format!("dev buy {dev:.2}%, token-sized"));
        } else if dev <= 6.0 {
            add(15, &format!("dev buy {dev:.2}%, inside the 1–6% band"));
        } else if dev <= 10.0 {
            add(0, &format!("dev buy {dev:.2}%, heavy"));
        } else {
            add(-25, &format!("dev buy {dev:.2}%, over 10%"));
        }
    }

    if let Some(rec) = &intel.record {
        let tax = rec.creator_tax_bps as i32;
        if tax == 0 {
            add(5, "no creator tax");
        } else if tax <= 200 {
            add(10, &format!("creator tax {}%, creator earns on volume", tax as f64 / 100.0));
        } else if tax <= 500 {
            add(-5, &format!("creator tax {}%", tax as f64 / 100.0));
        } else {
            add(-25, &format!("creator tax {}%, traders pay {}% per side", tax as f64 / 100.0, 1.0 + tax as f64 / 100.0));
        }
        if let Some(tx) = &intel.tx {
            if rec.creator_fee_recipient != tx.from {
                add(5, "fees routed to a third party (builder / KOL deal pattern)");
            }
        }
        if ctx.fee_recipient_is_contract == Some(true) {
            add(-8, "fee recipient is a contract");
        }
    }

    let soc = has_socials(intel.meta.as_ref());
    if !soc.any {
        add(-15, "no socials");
    } else {
        if soc.twitter {
            add(8, "has X link");
        }
        if soc.website {
            add(8, "has website");
        }
        if soc.telegram {
            add(8, "has telegram");
        }
    }
    if intel.meta.as_ref().map(|m| m.description.len()).unwrap_or(0) >= 40 {
        add(4, "real description");
    }

    if let Some(tx) = &intel.tx {
        let n = tx.exemptions.len();
        if n == 0 {
            add(5, "no declared bundle wallets");
        } else if n <= 3 {
            add(-5, &format!("{n} wallet(s) exempt from the opening tax"));
        } else {
            add(-20, &format!("{n} wallets exempt from the opening tax, declared bundle"));
        }
    }

    if ctx.farm_twins >= 2 {
        add(-25, &format!("launch farm: {} launches with this exact fingerprint in 30 min", ctx.farm_twins + 1));
    } else if ctx.farm_twins == 1 {
        add(-8, "one earlier launch with this exact fingerprint in 30 min");
    }

    if let Some((prior, graduated)) = ctx.deployer {
        if prior == 0 {
            add(5, "fresh deployer");
        } else if graduated as f64 / prior as f64 >= 0.3 {
            add(15, &format!("deployer graduated {graduated}/{prior} recent launches"));
        } else if prior >= 5 && graduated == 0 {
            add(-25, &format!("serial deployer, {prior} launches, none graduated"));
        } else {
            add(-5, &format!("deployer {prior} recent launches, {graduated} graduated"));
        }
    }

    if let (Some(a), Some(curve)) = (&ctx.activity, &intel.curve) {
        if a.unique_buyers >= 10 {
            add(10, &format!("{} distinct buyers", a.unique_buyers));
        } else if a.unique_buyers >= 4 {
            add(5, &format!("{} distinct buyers", a.unique_buyers));
        }
        if a.buys > 0 && a.taxed_buys == a.buys {
            add(-10, "every buy so far paid the opening tax (bots only)");
        }
        if a.sells > a.buys && a.buys > 3 {
            add(-10, "more sells than buys");
        }
        let p = progress(curve);
        let age = ctx.age_sec.unwrap_or(999);
        if p >= 0.25 && age <= 120 {
            add(10, &format!("{}% of the curve filled in {age}s", (p * 100.0).round()));
        }
    }

    let total = s.clamp(0, 100);
    let verdict = if total >= ctx.fire_th() {
        Verdict::Fire
    } else if total >= ctx.watch_th() {
        Verdict::Watch
    } else {
        Verdict::Skip
    };
    Score { total, verdict, reasons: r }
}
