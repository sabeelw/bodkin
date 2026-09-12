use alloy::primitives::U256;

pub fn short(a: &str, n: usize) -> String {
    if a.len() > 2 * n + 2 {
        format!("{}…{}", &a[..2 + n], &a[a.len() - n..])
    } else {
        a.to_string()
    }
}

pub fn eth(wei: U256) -> String {
    eth_digits(wei, 4)
}

pub fn eth_digits(wei: U256, digits: usize) -> String {
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
    let Some(v) = v.filter(|x| x.is_finite()) else { return "—".into() };
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
    if b % 100 == 0 {
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

pub fn ago(ts_sec: u64, now: f64) -> String {
    let d = (now - ts_sec as f64).max(0.0).floor() as u64;
    if d < 60 {
        format!("{d}s")
    } else if d < 3600 {
        format!("{}m {}s", d / 60, d % 60)
    } else if d < 86400 {
        format!("{}h {}m", d / 3600, (d % 3600) / 60)
    } else {
        format!("{}d {}h", d / 86400, (d % 86400) / 3600)
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

pub fn pad(s: &str, n: usize) -> String {
    if s.len() >= n {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(n - s.len()))
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub fn wei_to_f64(v: U256) -> f64 {
    let mut acc = 0.0f64;
    let mut base = 1.0f64;
    for limb in v.as_limbs() {
        acc += *limb as f64 * base;
        base *= 2.0f64.powi(64);
    }
    acc
}

pub fn parse_u256_str(s: &str) -> U256 {
    s.parse().unwrap_or(U256::ZERO)
}
