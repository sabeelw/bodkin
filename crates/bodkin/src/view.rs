use crate::fmt::{bps, clean_text, eth, hhmmss, pad, short, usd};
use crate::links::{Links, osc};
use crate::pons::curve::{fdv_quote, progress, spot_price};
use crate::pons::enrich::{CurveActivity, LaunchIntel, dev_share_pct, has_socials};
use crate::score::{Score, Verdict};
use crate::style::{loss, muted, neon, on_neon, white};

pub struct ViewCtx {
    pub eth_usd: Option<f64>,
    pub deployer: Option<(u32, u32)>,
    pub activity: Option<CurveActivity>,
}

pub fn verdict_tag(v: Verdict) -> String {
    match v {
        Verdict::Fire => on_neon(" FIRE "),
        Verdict::Watch => white("WATCH "),
        Verdict::Skip => muted("SKIP  "),
    }
}

pub fn launch_card(intel: &LaunchIntel, score: &Score, ctx: &ViewCtx, links: &Links) -> String {
    let ev = &intel.ev;
    let name = intel
        .meta
        .as_ref()
        .map(|m| clean_text(&m.name, 40))
        .unwrap_or_else(|| "(unreadable)".into());
    let sym = intel
        .meta
        .as_ref()
        .map(|m| format!("${}", clean_text(&m.symbol, 16)))
        .unwrap_or_default();
    let ts = intel
        .tx
        .as_ref()
        .map(|t| hhmmss(Some(t.timestamp)))
        .unwrap_or_else(|| hhmmss(None));
    let token = format!("{:#x}", ev.token);
    let curve = format!("{:#x}", ev.curve);
    let mut lines = vec![format!(
        "{}  {}  {}  {}  {} {}",
        muted(&ts),
        white(name),
        neon(osc(&sym, &Links::axiom(&curve))),
        muted(osc(&short(&token, 4), &Links::fomo(&token))),
        verdict_tag(score.verdict),
        white(score.total.to_string())
    )];

    let dev = intel
        .tx
        .as_ref()
        .map(|tx| {
            format!(
                "{:.2}% ({:.4} {})",
                dev_share_pct(Some(tx)),
                crate::fmt::wei_to_f64(tx.dev_buy_wei) / 10f64.powi(intel.pair.decimals as i32),
                clean_text(&intel.pair.symbol, 16)
            )
        })
        .unwrap_or_else(|| "?".into());
    let tax = intel
        .record
        .as_ref()
        .map(|r| bps(r.creator_tax_bps as u64))
        .unwrap_or_else(|| "?".into());
    let fee_to = match (&intel.record, &intel.tx) {
        (Some(rec), Some(tx)) if rec.creator_fee_recipient == tx.from => "deployer".into(),
        (Some(rec), Some(_)) => format!(
            "{} {}",
            short(&format!("{:#x}", rec.creator_fee_recipient), 4),
            neon("third party")
        ),
        _ => "?".into(),
    };
    let ex = intel.tx.as_ref().map(|t| t.exemptions.len()).unwrap_or(0);
    lines.push(format!(
        "   dev buy {}   creator tax {}   fees → {fee_to}   exempt wallets {}",
        white(dev),
        white(tax),
        if ex > 0 {
            loss(ex.to_string())
        } else {
            white("0")
        }
    ));

    let soc = has_socials(intel.meta.as_ref());
    let mut soc_bits = Vec::new();
    if soc.twitter {
        soc_bits.push("x");
    }
    if soc.website {
        soc_bits.push("web");
    }
    if soc.telegram {
        soc_bits.push("tg");
    }
    let soc_txt = if soc_bits.is_empty() {
        loss("none")
    } else {
        soc_bits.join(" ")
    };
    let desc = intel
        .meta
        .as_ref()
        .map(|m| clean_text(&m.description.replace('\n', " "), 96))
        .unwrap_or_default();
    let desc = if desc.chars().count() == 96 {
        format!("{desc}…")
    } else {
        desc
    };
    lines.push(format!("   socials {soc_txt}   {}", muted(&desc)));

    if let Some((prior, graduated)) = ctx.deployer {
        lines.push(format!(
            "   deployer {}  {prior} prior launch{} in ~2d, {graduated} graduated",
            muted(short(&format!("{:#x}", ev.deployer), 4)),
            if prior == 1 { "" } else { "es" }
        ));
    }
    if let Some(curve_s) = &intel.curve {
        if intel.record.as_ref().is_some_and(|r| r.phase != 0) {
            let phase = ["curve", "swept", "v4 pool", "rescued"][intel
                .record
                .as_ref()
                .map(|r| r.phase.min(3) as usize)
                .unwrap_or(0)];
            lines.push(format!(
                "   {} → {phase}  {}",
                neon("graduated"),
                muted("curve reserves are empty after graduation; price now lives in the pool")
            ));
        } else {
            let p = progress(curve_s);
            let filled = (p * 20.0).round() as usize;
            let bar = format!("{}{}", "█".repeat(filled), "░".repeat(20 - filled));
            let fdv = fdv_quote(curve_s);
            let open = if !curve_s.opening_tax_bps.is_zero() {
                loss(format!(
                    "opening tax {}",
                    bps(curve_s.opening_tax_bps.try_into().unwrap_or(0))
                ))
            } else {
                muted("opening tax 0")
            };
            let fdv_usd = ctx.eth_usd.map(|px| fdv * px);
            lines.push(format!(
                "   curve {} {:.1}%  {}/{} {}   fdv {:.2} {} {}   {open}",
                neon(bar),
                p * 100.0,
                eth(curve_s.real_quote_reserve),
                eth(curve_s.graduation_threshold),
                clean_text(&intel.pair.symbol, 16),
                fdv,
                clean_text(&intel.pair.symbol, 16),
                muted(usd(fdv_usd))
            ));
        }
    }
    if let Some(a) = &ctx.activity {
        lines.push(format!(
            "   flow {} buys / {} sells, {} buyers, {} taxed, net {} ETH",
            a.buys,
            a.sells,
            a.unique_buyers,
            a.taxed_buys,
            eth(a.quote_in.saturating_sub(a.quote_out))
        ));
    }
    lines.push(format!(
        "   {}",
        muted(clean_text(&score.reasons.join(" · "), 2048))
    ));
    let open = [
        osc("pons", &Links::pons(&token)),
        osc("axiom", &Links::axiom(&curve)),
        osc("fomo", &Links::fomo(&token)),
        osc("explorer", &Links::explorer(&token)),
    ]
    .into_iter()
    .map(neon)
    .collect::<Vec<_>>()
    .join(" · ");
    lines.push(format!(
        "   {} {open}   {}",
        muted("open:"),
        muted("ctrl+click in Windows Terminal, iTerm, kitty, VS Code")
    ));
    let _ = links;
    lines.join("\n")
}

pub fn launch_update(
    intel: &LaunchIntel,
    score: &Score,
    activity: &CurveActivity,
    age_sec: u64,
    ctx: &ViewCtx,
) -> String {
    let sym = intel
        .meta
        .as_ref()
        .map(|m| format!("${}", clean_text(&m.symbol, 16)))
        .unwrap_or_else(|| short(&format!("{:#x}", intel.ev.token), 4));
    let p = intel.curve.as_ref().map(progress).unwrap_or(0.0);
    let px = intel.curve.as_ref().map(spot_price).unwrap_or(0.0);
    let fdv = intel.curve.as_ref().map(fdv_quote).unwrap_or(0.0);
    let fdv_usd = ctx.eth_usd.map(|u| fdv * u);
    let age = format!("{age_sec}s");
    let total = format!("{:>3}", score.total);
    format!(
        "{}  {} +{} {} {}  curve {:>5.1}%  fdv {:.2} ETH {}  {}b/{}s {} buyers  px {:.2e}",
        muted(hhmmss(None)),
        pad(&sym, 10),
        pad(&age, 4),
        verdict_tag(score.verdict),
        total,
        p * 100.0,
        fdv,
        muted(usd(fdv_usd)),
        activity.buys,
        activity.sells,
        activity.unique_buyers,
        px
    )
}
