/// Live factory snapshot on 2026-09-12 (owner-mutable, max 60 s). Each curve copies these in `initialize`.
pub const LIVE_START_BPS: u64 = 9900;
pub const LIVE_WINDOW_SECS: u64 = 3;

/// `tax = startBps >> floor(14 * elapsed / window)` with `elapsed = block.timestamp − launchedAt`.
/// After `window` seconds the tax is 0. Any ceiling in [19, 617] is the same rule as 300: enter in second +2.
pub fn snipe_tax_bps(start_bps: u64, window_secs: u64, elapsed_secs: u64) -> u64 {
    if window_secs == 0 || elapsed_secs >= window_secs {
        return 0;
    }
    let shift = 14u64.saturating_mul(elapsed_secs) / window_secs;
    if shift >= 64 {
        0
    } else {
        start_bps >> shift
    }
}

/// Unix second at which `entry_second` begins (inclusive). Entry second 2 → `launched_at + 2`.
pub fn boundary_instant(launched_at: u64, entry_second: u64) -> u64 {
    launched_at.saturating_add(entry_second)
}

/// Live 3 s table: 9900 / 618 / 19 / 0 at e=0/1/2/≥3.
pub fn live_table(elapsed_secs: u64) -> u64 {
    snipe_tax_bps(LIVE_START_BPS, LIVE_WINDOW_SECS, elapsed_secs)
}

/// First ceiling that is legal at `entry_second` on the live staircase.
pub fn tax_at_entry(start_bps: u64, window_secs: u64, entry_second: u64) -> u64 {
    snipe_tax_bps(start_bps, window_secs, entry_second)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_staircase() {
        assert_eq!(live_table(0), 9900);
        assert_eq!(live_table(1), 618);
        assert_eq!(live_table(2), 19);
        assert_eq!(live_table(3), 0);
        assert_eq!(live_table(10), 0);
    }

    #[test]
    fn ceiling_band() {
        // 19..=617 is the same entry second as 300 on the live 3 s window.
        assert!(live_table(1) > 617);
        assert!(live_table(2) <= 19);
        assert_eq!(boundary_instant(1_000, 2), 1_002);
    }
}
