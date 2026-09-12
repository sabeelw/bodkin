use crate::chain::{
    DEFAULT_HTTP_OFFICIAL, DEFAULT_HTTP_PUBLICNODE, DEFAULT_SEQUENCER, DEFAULT_WS_PUBLICNODE,
};
use alloy::primitives::{Address, U256};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::OnceLock;

/// `.env` entries live in this overlay rather than process env (`set_var` is
/// `unsafe` on edition 2024 and races other readers). Real env still wins.
static ENV_OVERLAY: OnceLock<HashMap<String, String>> = OnceLock::new();

/// Process env wins; dotenvy handles quoting, comments, escapes and interpolation.
pub fn load_env_file(path: impl AsRef<Path>) {
    let Ok(entries) = dotenvy::from_path_iter(path) else {
        return;
    };
    let mut overlay = HashMap::new();
    for entry in entries {
        match entry {
            Ok((key, value)) if std::env::var_os(&key).is_none() => {
                overlay.insert(key, value);
            }
            Ok(_) => {}
            Err(error) => eprintln!("warning: invalid .env entry: {error}"),
        }
    }
    // First loader wins; Config::from_env calls this once at process start.
    let _ = ENV_OVERLAY.set(overlay);
}

fn overlay_get(key: &str) -> Option<String> {
    ENV_OVERLAY.get().and_then(|m| m.get(key)).cloned()
}

pub fn env_str(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .or_else(|| overlay_get(key))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn strict_env<T: FromStr>(key: &str, fallback: T, valid: impl Fn(&T) -> bool) -> anyhow::Result<T> {
    let Some(raw) = env_str(key) else {
        return Ok(fallback);
    };
    let value = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("{key}={raw:?} is invalid"))?;
    anyhow::ensure!(
        valid(&value),
        "{key}={raw:?} is outside the supported range"
    );
    Ok(value)
}

fn env_parse<T: FromStr>(key: &str, fallback: T, ok: impl Fn(&T) -> bool) -> T {
    match env_str(key) {
        Some(v) => match v.parse() {
            Ok(x) if ok(&x) => x,
            _ => {
                eprintln!("warning: {key}={v:?} is invalid, using default");
                fallback
            }
        },
        None => fallback,
    }
}

pub fn env_num(key: &str, fallback: f64) -> f64 {
    env_parse(key, fallback, |v: &f64| v.is_finite())
}

pub fn env_u64(key: &str, fallback: u64) -> u64 {
    env_parse(key, fallback, |_| true)
}

pub fn strict_env_u64(key: &str, fallback: u64) -> anyhow::Result<u64> {
    strict_env(key, fallback, |_| true)
}

pub fn strict_env_num(key: &str, fallback: f64) -> anyhow::Result<f64> {
    strict_env(key, fallback, |value: &f64| value.is_finite())
}

pub fn strict_env_bool(key: &str, fallback: bool) -> anyhow::Result<bool> {
    let Some(value) = env_str(key) else {
        return Ok(fallback);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{key}={value:?} is not a boolean"),
    }
}

pub fn env_bool(key: &str, fallback: bool) -> bool {
    match env_str(key) {
        Some(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        None => fallback,
    }
}

pub fn parse_ether(value: &str) -> anyhow::Result<U256> {
    let value = value.trim();
    anyhow::ensure!(!value.is_empty(), "empty ether amount");
    anyhow::ensure!(!value.starts_with('-'), "negative ether amount");
    if let Some((_, fraction)) = value.split_once('.') {
        anyhow::ensure!(fraction.len() <= 18, "more than 18 decimal places");
    }
    Ok(alloy::primitives::utils::parse_ether(value)?)
}

#[derive(Debug, Clone)]
pub struct EndpointCfg {
    pub url: String,
    pub logs: bool,
    pub label: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc_http: Vec<EndpointCfg>,
    pub rpc_ws: Vec<String>,
    pub sequencer_url: String,
    pub helper: Option<Address>,
    pub poll_ms: u64,
    pub rpc_in_flight: usize,
    pub rpc_spacing_ms: u64,
    pub rpc_logs_spacing_ms: u64,
    pub board_port: u16,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        load_env_file(".env");
        let sequencer_url = env_str("SEQUENCER_URL").unwrap_or_else(|| DEFAULT_SEQUENCER.into());
        validate_url("SEQUENCER_URL", &sequencer_url, &["http", "https"])?;
        let helper = env_str("HELPER_ADDRESS")
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("HELPER_ADDRESS={value:?} is not an address"))
            })
            .transpose()?;
        Ok(Self {
            rpc_http: parse_rpc_urls()?,
            rpc_ws: parse_ws_urls()?,
            sequencer_url,
            helper,
            poll_ms: strict_env("POLL_MS", 300u64, |value| (10..=60_000).contains(value))?,
            rpc_in_flight: strict_env("RPC_IN_FLIGHT", 3usize, |value| (1..=64).contains(value))?,
            rpc_spacing_ms: strict_env("RPC_SPACING_MS", 50u64, |value| *value <= 60_000)?,
            rpc_logs_spacing_ms: strict_env("RPC_LOGS_SPACING_MS", 400u64, |value| {
                *value <= 60_000
            })?,
            board_port: strict_env("BOARD_PORT", 4663u16, |value| *value > 0)?,
        })
    }

    pub fn http_labels(&self) -> String {
        self.rpc_http
            .iter()
            .map(|e| e.label.as_str())
            .collect::<Vec<_>>()
            .join(" → ")
    }

    pub fn ws_labels(&self) -> String {
        if self.rpc_ws.is_empty() {
            format!("off, poll {} ms", self.poll_ms)
        } else {
            self.rpc_ws.join(", ")
        }
    }
}

fn parse_rpc_urls() -> anyhow::Result<Vec<EndpointCfg>> {
    let defaults = vec![
        EndpointCfg {
            url: DEFAULT_HTTP_PUBLICNODE.into(),
            logs: false,
            label: "publicnode".into(),
        },
        EndpointCfg {
            url: DEFAULT_HTTP_OFFICIAL.into(),
            logs: true,
            label: "robinhood".into(),
        },
    ];
    let Some(raw) = env_str("RPC_URL") else {
        return Ok(defaults);
    };
    let endpoints = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let no_logs = value.ends_with("#nologs");
            let url = value.strip_suffix("#nologs").unwrap_or(value).to_string();
            let parsed = validate_url("RPC_URL", &url, &["http", "https"])?;
            let known = defaults.iter().find(|default| default.url == url);
            Ok(EndpointCfg {
                url,
                logs: known.map(|default| default.logs).unwrap_or(!no_logs),
                label: known
                    .map(|default| default.label.clone())
                    .unwrap_or_else(|| parsed.host_str().expect("validated URL host").to_string()),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(!endpoints.is_empty(), "RPC_URL contains no endpoints");
    Ok(endpoints)
}

fn parse_ws_urls() -> anyhow::Result<Vec<String>> {
    let urls = match env_str("RPC_WS_URL") {
        None => vec![DEFAULT_WS_PUBLICNODE.into()],
        Some(value) if value.eq_ignore_ascii_case("off") => vec![],
        Some(value) => value
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_owned)
            .collect(),
    };
    for url in &urls {
        validate_url("RPC_WS_URL", url, &["ws", "wss"])?;
    }
    Ok(urls)
}

fn validate_url(key: &str, value: &str, schemes: &[&str]) -> anyhow::Result<url::Url> {
    let url = url::Url::parse(value)
        .map_err(|error| anyhow::anyhow!("{key}={value:?} is invalid: {error}"))?;
    anyhow::ensure!(
        schemes.contains(&url.scheme()) && url.host_str().is_some(),
        "{key}={value:?} must use one of {} and include a host",
        schemes.join(", ")
    );
    Ok(url)
}

/// `34@100,33@300` → [(34, 100), (33, 300)]. Fraction of the remaining bag at that gain %.
/// Malformed entries and out-of-range fractions are dropped with a warning instead
/// of silently narrowing the ladder.
pub fn parse_exit_ladder(raw: &str) -> Vec<(u32, i32)> {
    raw.split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            let Some((frac, at)) = part.split_once('@') else {
                eprintln!("warning: EXIT_LADDER entry {part:?} has no `frac@pct`, skipped");
                return None;
            };
            match (frac.trim().parse::<u32>(), at.trim().parse::<i32>()) {
                (Ok(f), Ok(a)) if (1..=100).contains(&f) => Some((f, a)),
                (Ok(f), Ok(_)) => {
                    eprintln!("warning: EXIT_LADDER fraction {f}% outside 1..=100, skipped");
                    None
                }
                _ => {
                    eprintln!("warning: EXIT_LADDER entry {part:?} is not `frac@pct`, skipped");
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ether_strict() {
        assert_eq!(
            parse_ether("0.01").unwrap(),
            U256::from(10_000_000_000_000_000u128)
        );
        assert_eq!(
            parse_ether("2").unwrap(),
            U256::from(2_000_000_000_000_000_000u128)
        );
        assert!(parse_ether("-1").is_err());
        assert!(parse_ether("").is_err());
        assert!(parse_ether("0.0000000000000000001").is_err()); // > 18 dp
        assert!(parse_ether("abc").is_err());
    }

    #[test]
    fn ladder_drops_malformed_not_silently_narrow() {
        assert_eq!(
            parse_exit_ladder("34@100,33@300"),
            vec![(34, 100), (33, 300)]
        );
        assert_eq!(
            parse_exit_ladder("34@100,junk,10@50"),
            vec![(34, 100), (10, 50)]
        );
        assert!(parse_exit_ladder("150@100").is_empty()); // fraction > 100
        assert!(parse_exit_ladder("0@50").is_empty());
        assert!(parse_exit_ladder("").is_empty());
    }

    #[test]
    fn endpoint_urls_are_typed_and_scheme_limited() {
        assert!(validate_url("RPC_URL", "https://rpc.example", &["http", "https"]).is_ok());
        assert!(validate_url("RPC_URL", "ftp://rpc.example", &["http", "https"]).is_err());
        assert!(validate_url("RPC_WS_URL", "wss://rpc.example", &["ws", "wss"]).is_ok());
        assert!(validate_url("RPC_WS_URL", "not a url", &["ws", "wss"]).is_err());
    }

    #[test]
    fn env_num_rejects_nan_and_inf() {
        // Unique keys — env is process-global.
        unsafe {
            std::env::set_var("BODKIN_T_NAN", "NaN");
            std::env::set_var("BODKIN_T_INF", "inf");
            std::env::set_var("BODKIN_T_OK", "1.5");
        }
        assert_eq!(env_num("BODKIN_T_NAN", 7.0), 7.0);
        assert_eq!(env_num("BODKIN_T_INF", 7.0), 7.0);
        assert_eq!(env_num("BODKIN_T_OK", 7.0), 1.5);
        unsafe {
            std::env::remove_var("BODKIN_T_NAN");
            std::env::remove_var("BODKIN_T_INF");
            std::env::remove_var("BODKIN_T_OK");
        }
    }
}
