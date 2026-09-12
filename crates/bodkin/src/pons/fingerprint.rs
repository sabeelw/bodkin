use super::enrich::LaunchIntel;
use std::collections::HashMap;

/// Launch farms: brand-new wallets sharing one fingerprint (dev-buy wei, creator tax, links, exemption count).
pub struct FarmDetector {
    seen: HashMap<String, Vec<(u64, String)>>,
    window_ms: u64,
}

impl Default for FarmDetector {
    fn default() -> Self {
        Self::new(30 * 60_000)
    }
}

impl FarmDetector {
    pub fn new(window_ms: u64) -> Self {
        Self { seen: HashMap::new(), window_ms }
    }

    pub fn key(intel: &LaunchIntel) -> Option<String> {
        let tx = intel.tx.as_ref()?;
        let rec = intel.record.as_ref()?;
        let s = intel.meta.as_ref().map(|m| &m.socials);
        let links = format!(
            "{}{}{}",
            if s.is_some_and(|x| !x.twitter.trim().is_empty()) { 1 } else { 0 },
            if s.is_some_and(|x| !x.website.trim().is_empty()) { 1 } else { 0 },
            if s.is_some_and(|x| !x.telegram.trim().is_empty()) { 1 } else { 0 },
        );
        Some(format!("{}|{}|{}|{}", tx.dev_buy_wei, rec.creator_tax_bps, links, tx.exemptions.len()))
    }

    /// Records the launch and returns how many earlier launches in the window carried the same fingerprint from another deployer.
    pub fn note(&mut self, intel: &LaunchIntel, now_ms: u64) -> (u32, Option<String>) {
        let Some(key) = Self::key(intel) else { return (0, None) };
        let window = self.window_ms;
        let list = self.seen.entry(key.clone()).or_default();
        list.retain(|(t, _)| *t > now_ms.saturating_sub(window));
        let me = intel.ev.deployer.to_string().to_ascii_lowercase();
        let twins = list.iter().filter(|(_, d)| d != &me).count() as u32;
        list.push((now_ms, me));
        if self.seen.len() > 5_000 {
            self.seen.retain(|_, v| v.iter().any(|(t, _)| *t > now_ms.saturating_sub(window)));
        }
        (twins, Some(key))
    }
}
