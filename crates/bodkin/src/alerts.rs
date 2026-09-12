pub async fn eth_usd() -> Option<f64> {
    static CACHE: std::sync::Mutex<Option<(f64, u64)>> = std::sync::Mutex::new(None);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    if let Some((v, at)) = *CACHE.lock().ok()?
        && now.saturating_sub(at) < 60_000
    {
        return Some(v);
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;
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
