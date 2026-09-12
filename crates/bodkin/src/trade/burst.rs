use super::submitter::{SendOutcome, Submitter};
use super::wallet::Wallet;
use crate::pons::clock::ChainClock;
use crate::pons::tax::boundary_instant;
use alloy::primitives::{Address, Bytes, B256, U256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BurstClass {
    Fill,
    HelperRevert,
    LateDup,
    NonceGap,
    ShadowWouldLand,
}

#[derive(Debug, Clone)]
pub struct SignedTx {
    pub nonce: u64,
    pub hash: B256,
    pub raw: Bytes,
}

#[derive(Debug, Clone)]
pub struct BurstPlan {
    pub txs: Vec<SignedTx>,
    pub fire_at_unix: u64,
    pub lead_ms: u64,
}

#[derive(Debug, Clone)]
pub struct BurstResult {
    pub class: BurstClass,
    pub hash: Option<B256>,
    pub attempt: usize,
    pub landed_block: Option<u64>,
    pub message: String,
}

pub fn classify_send(out: &SendOutcome, first_ok: bool) -> BurstClass {
    match out {
        SendOutcome::Hash(_) if first_ok => BurstClass::Fill,
        SendOutcome::Known => BurstClass::LateDup,
        SendOutcome::Revert { .. } => BurstClass::HelperRevert,
        SendOutcome::Reject { message, .. } if message.contains("nonce") => BurstClass::NonceGap,
        _ => BurstClass::NonceGap,
    }
}

pub struct BurstCtl {
    pub max: usize,
    pub lead_ms: AtomicU64,
}

impl BurstCtl {
    pub fn from_env() -> Self {
        Self {
            max: crate::config::env_u64("BURST_MAX", 8).clamp(1, 16) as usize,
            lead_ms: AtomicU64::new(crate::config::env_u64("BURST_LEAD_MS", 150)),
        }
    }

    pub fn lead_ms(&self) -> u64 {
        self.lead_ms.load(Ordering::Relaxed)
    }

    /// Adaptive lead from which attempt landed in the first +2 block.
    pub fn adapt(&self, attempt: usize, landed_in_first: bool) {
        let cur = self.lead_ms();
        let next = if landed_in_first && attempt == 0 {
            cur.saturating_sub(10).max(40)
        } else if !landed_in_first {
            (cur + 20).min(400)
        } else {
            cur
        };
        self.lead_ms.store(next, Ordering::Relaxed);
    }
}

pub fn presign_burst(
    wallet: &Wallet,
    clock: &ChainClock,
    helper: Address,
    calldata: Bytes,
    value: U256,
    n: usize,
) -> anyhow::Result<Vec<SignedTx>> {
    let nonces = wallet.take_nonces(n as u32);
    let mut out = Vec::with_capacity(n);
    for nonce in nonces {
        let (hash, raw) = wallet.sign_eip1559(helper, value, calldata.clone(), nonce, clock)?;
        out.push(SignedTx { nonce, hash, raw });
    }
    Ok(out)
}

/// Fire all signed txs in the same millisecond. Hedge: spray nonce 0 to all 3 IPs.
pub async fn fire_burst(sub: &Submitter, txs: &[SignedTx], spray_first: bool) -> BurstResult {
    if txs.is_empty() {
        return BurstResult { class: BurstClass::NonceGap, hash: None, attempt: 0, landed_block: None, message: "empty burst".into() };
    }
    let t0 = Instant::now();
    let mut futs = Vec::new();
    for (i, tx) in txs.iter().enumerate() {
        if i == 0 && spray_first {
            futs.push(futures::future::Either::Left(async move { (0usize, sub.spray(&tx.raw).await) }));
        } else {
            let raw = tx.raw.clone();
            futs.push(futures::future::Either::Right(async move { (i, sub.send_raw(&raw).await) }));
        }
    }
    let results = futures::future::join_all(futs).await;
    for (i, out) in results {
        match out {
            SendOutcome::Hash(h) => {
                return BurstResult {
                    class: BurstClass::Fill,
                    hash: Some(h),
                    attempt: i,
                    landed_block: None,
                    message: format!("fill attempt {i} in {} ms", t0.elapsed().as_millis()),
                };
            }
            SendOutcome::Revert { hash, message } => {
                return BurstResult {
                    class: BurstClass::HelperRevert,
                    hash,
                    attempt: i,
                    landed_block: None,
                    message,
                };
            }
            SendOutcome::Known => {
                return BurstResult {
                    class: BurstClass::LateDup,
                    hash: Some(txs[i].hash),
                    attempt: i,
                    landed_block: None,
                    message: "already known".into(),
                };
            }
            _ => {}
        }
    }
    BurstResult { class: BurstClass::NonceGap, hash: None, attempt: 0, landed_block: None, message: "no inclusion".into() }
}

/// Sleep until `launched_at + entry_second` minus lead, then fire.
pub async fn clock_fire(clock: &ChainClock, launched_at: u64, entry_second: u64, lead_ms: u64, sub: &Submitter, txs: &[SignedTx]) -> BurstResult {
    let boundary = boundary_instant(launched_at, entry_second);
    let target = boundary.saturating_mul(1000).saturating_sub(lead_ms);
    let now = crate::pons::clock::now_ms();
    let adj = (now as i64 + clock.offset_ms()) as u64;
    if target > adj {
        tokio::time::sleep(std::time::Duration::from_millis(target - adj)).await;
    }
    fire_burst(sub, txs, true).await
}

/// Dry-run shadow: send nothing; from newHeads, report which attempt would have been first.
pub fn shadow_result(launched_at: u64, entry_second: u64, first_plus2_block: u64, our_would_block: Option<u64>) -> BurstResult {
    let first = first_plus2_block;
    let land = our_would_block.unwrap_or(first + 3);
    let ok = land == first;
    BurstResult {
        class: BurstClass::ShadowWouldLand,
        hash: None,
        attempt: 0,
        landed_block: Some(land),
        message: if ok {
            format!("would land in the first block of second {entry_second} (block {first}, launchedAt {launched_at})")
        } else {
            format!("would land in block {land}, first +{entry_second} block is {first}")
        },
    }
}

pub fn encode_buy_once(curve: Address, token: Address, recipient: Address, max_tax_bps: U256, min_tokens_out: U256, max_real_quote: U256) -> Bytes {
    use alloy::sol_types::SolCall;
    Bytes::from(
        crate::abi::helper::buyOnceCall {
            curve,
            token,
            recipient,
            maxTaxBps: max_tax_bps,
            minTokensOut: min_tokens_out,
            maxRealQuote: max_real_quote,
        }
        .abi_encode(),
    )
}

pub fn _arc<T>(x: T) -> Arc<T> {
    Arc::new(x)
}
