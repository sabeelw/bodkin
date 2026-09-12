use super::enrich::LaunchIntel;
use std::collections::HashMap;

/// Launch farms: brand-new wallets sharing one fingerprint — dev-buy wei in
/// quote-asset units, the pair it was paid in, creator tax, the actual social
/// links (normalized), and the declared exemption count.
pub struct FarmDetector {
    /// fingerprint → (seen_at_ms, deployer, token)
    seen: HashMap<String, Vec<(u64, String, String)>>,
    window_ms: u64,
}

impl Default for FarmDetector {
    fn default() -> Self {
        Self::new(30 * 60_000)
    }
}

fn norm_link(s: &str) -> String {
    let mut x = s.trim().to_ascii_lowercase();
    for p in ["https://", "http://"] {
        if let Some(rest) = x.strip_prefix(p) {
            x = rest.to_string();
        }
    }
    if let Some(rest) = x.strip_prefix("www.") {
        x = rest.to_string();
    }
    while x.ends_with('/') {
        x.pop();
    }
    x
}

impl FarmDetector {
    pub fn new(window_ms: u64) -> Self {
        Self {
            seen: HashMap::new(),
            window_ms,
        }
    }

    pub fn key(intel: &LaunchIntel) -> Option<String> {
        let tx = intel.tx.as_ref()?;
        let rec = intel.record.as_ref()?;
        let s = intel.meta.as_ref().map(|m| &m.socials);
        let link = |f: &dyn Fn(&super::enrich::Socials) -> &String| {
            s.map(|x| norm_link(f(x))).unwrap_or_default()
        };
        Some(format!(
            "{}|{}|{}|{}|{}|{}|{}",
            tx.dev_buy_wei,
            format_args!("{:#x}", intel.pair.address),
            rec.creator_tax_bps,
            link(&|x| &x.twitter),
            link(&|x| &x.website),
            link(&|x| &x.telegram),
            tx.exemptions.len()
        ))
    }

    /// Records the launch and returns how many earlier launches in the window
    /// carried the same fingerprint from another deployer. The same
    /// (deployer, token) pair noted twice — e.g. a backfill and the live event —
    /// only counts once.
    pub fn note(&mut self, intel: &LaunchIntel, now_ms: u64) -> (u32, Option<String>) {
        let Some(key) = Self::key(intel) else {
            return (0, None);
        };
        let window = self.window_ms;
        let list = self.seen.entry(key.clone()).or_default();
        list.retain(|(t, _, _)| *t > now_ms.saturating_sub(window));
        let me = intel.ev.deployer.to_string().to_ascii_lowercase();
        let token = format!("{:#x}", intel.ev.token);
        let twins = list.iter().filter(|(_, d, _)| d != &me).count() as u32;
        if !list.iter().any(|(_, d, tok)| d == &me && tok == &token) {
            list.push((now_ms, me, token));
        }
        if self.seen.len() > 5_000 {
            self.seen
                .retain(|_, v| v.iter().any(|(t, _, _)| *t > now_ms.saturating_sub(window)));
        }
        (twins, Some(key))
    }
}
