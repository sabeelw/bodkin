use crate::pons::clock::now_ms;
use alloy::primitives::U256;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

/// Attacker-controlled text must never reach the terminal raw: ESC can inject
/// OSC hyperlinks or rewrite the screen. Strip controls and cap by grapheme.
pub fn clean_text(value: &str, max_graphemes: usize) -> String {
    strip_ansi_escapes::strip_str(value)
        .graphemes(true)
        .filter(|grapheme| !grapheme.chars().any(char::is_control))
        .take(max_graphemes)
        .collect::<String>()
        .trim()
        .to_string()
}

pub fn first_line(value: &str) -> String {
    clean_text(value.lines().next().unwrap_or(value), 160)
}

pub fn short(value: &str, n: usize) -> String {
    let graphemes = value.graphemes(true).collect::<Vec<_>>();
    if graphemes.len() > 2 * n + 2 {
        format!(
            "{}…{}",
            graphemes[..2 + n].concat(),
            graphemes[graphemes.len() - n..].concat()
        )
    } else {
        value.to_string()
    }
}

pub fn eth(wei: U256) -> String {
    let digits = 4;
    let v = wei_to_f64(wei) / 1e18;
    if v == 0.0 {
        return "0".into();
    }
    if v < 0.0001 {
        return format!("{v:.2e}");
    }
    let s = if v < 1.0 {
        format!("{v:.digits$}")
    } else {
        format!("{v:.3}")
    };
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

pub fn usd(v: Option<f64>) -> String {
    let Some(v) = v.filter(|x| x.is_finite()) else {
        return "—".into();
    };
    let abs = v.abs();
    let sign = if v < 0.0 { "-" } else { "" };
    if abs >= 1_000_000.0 {
        format!("{sign}${:0.2}M", abs / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{sign}${:0.1}K", abs / 1_000.0)
    } else if abs >= 1.0 {
        format!("{sign}${:0.0}", abs)
    } else {
        format!("{sign}${:0.2}", abs)
    }
}

pub fn bps(b: u64) -> String {
    if b.is_multiple_of(100) {
        format!("{}%", b / 100)
    } else {
        format!("{:.2}%", b as f64 / 100.0)
    }
}

pub fn tokens(amount: U256) -> String {
    let v = wei_to_f64(amount) / 1e18;
    if v >= 1e9 {
        format!("{:.2}B", v / 1e9)
    } else if v >= 1e6 {
        format!("{:.1}M", v / 1e6)
    } else if v >= 1e3 {
        format!("{:.1}K", v / 1e3)
    } else {
        format!("{v:.0}")
    }
}

pub fn hhmmss(ts_sec: Option<u64>) -> String {
    let ms = ts_sec.map(|s| s * 1000).unwrap_or_else(now_ms);
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|t| t.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "??:??:??".into())
}

pub fn iso(ts_sec: u64) -> String {
    chrono::DateTime::from_timestamp(ts_sec as i64, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_default()
}

pub fn pad(value: &str, columns: usize) -> String {
    let width = value.width();
    format!("{value}{}", " ".repeat(columns.saturating_sub(width)))
}

pub fn wei_to_f64(value: U256) -> f64 {
    f64::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_helpers_respect_graphemes_and_column_width() {
        assert_eq!(short("0x👨‍👩‍👧‍👦abcdef", 2), "0x👨‍👩‍👧‍👦a…ef");
        assert_eq!(pad("界", 3), "界 ");
        assert_eq!(pad("e\u{301}", 2), "e\u{301} ");
        assert_eq!(clean_text("é界é界", 3), "é界é");
        assert_eq!(clean_text("x\u{1b}[31my\n\rz\u{7}", 32), "xyz");
        assert_eq!(
            clean_text("a\u{1b}]8;;https://evil.example\u{7}b\u{1b}]8;;\u{7}c", 32),
            "abc"
        );
        assert_eq!(first_line("x\u{1b}[31m\ny"), "x");
    }

    #[test]
    fn alloy_handles_full_width_numeric_conversion() {
        assert!(wei_to_f64(U256::MAX).is_finite());
        assert_eq!(wei_to_f64(U256::from(42)), 42.0);
    }
}
