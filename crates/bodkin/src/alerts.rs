use crate::fmt::eth;
use alloy::primitives::U256;

pub async fn send_telegram(text: &str) -> bool {
    let Some(token) = crate::config::env_str("TELEGRAM_BOT_TOKEN") else { return false };
    let Some(chat) = crate::config::env_str("TELEGRAM_CHAT_ID") else { return false };
    let client = match reqwest::Client::builder().timeout(std::time::Duration::from_secs(8)).build() {
        Ok(c) => c,
        Err(_) => return false,
    };
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    client
        .post(url)
        .json(&serde_json::json!({"chat_id": chat, "text": text, "disable_web_page_preview": true}))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

pub async fn notify(kind: &str, live: bool, symbol: &str, token: &str, eth_in: U256, tax_bps: u64, waited_ms: u64, eth_out: U256, pnl_pct: f64, venue: &str, reason: &str) {
    let text = if kind == "fire" {
        format!(
            "bodkin {} {symbol}: {} ETH at tax {:.2}% after {waited_ms} ms\nhttps://robinhoodchain.blockscout.com/token/{token}",
            if live { "FIRE" } else { "fire (dry)" },
            eth(eth_in),
            tax_bps as f64 / 100.0
        )
    } else {
        format!(
            "bodkin exit {symbol}: {} ETH ({:+.1}%) on {venue}, {reason}",
            eth(eth_out),
            pnl_pct
        )
    };
    let _ = send_telegram(&text).await;
}

pub async fn eth_usd() -> Option<f64> {
    static CACHE: std::sync::Mutex<Option<(f64, u64)>> = std::sync::Mutex::new(None);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?.as_millis() as u64;
    if let Some((v, at)) = *CACHE.lock().ok()? {
        if now.saturating_sub(at) < 60_000 {
            return Some(v);
        }
    }
    let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().ok()?;
    let r = client
        .get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd")
        .header("accept", "application/json")
        .send()
        .await
        .ok()?;
    if !r.status().is_success() {
        return CACHE.lock().ok().and_then(|g| g.map(|(v, _)| v));
    }
    let j: serde_json::Value = r.json().await.ok()?;
    let v = j.get("ethereum")?.get("usd")?.as_f64()?;
    if v > 0.0 {
        if let Ok(mut g) = CACHE.lock() {
            *g = Some((v, now));
        }
        Some(v)
    } else {
        None
    }
}
