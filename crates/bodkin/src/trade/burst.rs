use super::submitter::{SendOutcome, Submitter};
use super::wallet::Wallet;
use crate::pons::clock::ChainClock;
use crate::pons::tax::boundary_instant;
use alloy::primitives::{Address, B256, Bytes, U256};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BurstClass {
    /// A send was accepted with a hash. NOT a fill — reconcile via receipt
    /// before opening a position.
    Submitted,
    HelperRevert,
    LateDup,
    NonceGap,
    /// The armed flag went false between signing and the fire instant.
    Aborted,
    ShadowWouldLand,
}

#[derive(Debug, Clone)]
pub struct SignedTx {
    pub nonce: u64,
    pub hash: B256,
    pub raw: Bytes,
}

#[derive(Debug, Clone)]
pub struct BurstResult {
    pub class: BurstClass,
    pub hash: Option<B256>,
    pub attempt: usize,
    pub message: String,
    pub outcomes: Vec<(usize, SendOutcome)>,
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
    let start = nonces
        .first()
        .copied()
        .unwrap_or_else(|| wallet.peek_nonce());
    let mut out = Vec::with_capacity(n);
    for nonce in nonces {
        match wallet.sign_eip1559(helper, value, calldata.clone(), nonce, clock) {
            Ok((hash, raw)) => out.push(SignedTx { nonce, hash, raw }),
            Err(e) => {
                anyhow::ensure!(
                    wallet.rewind_nonces(start, n as u64),
                    "burst signing failed and its nonce range could not be rewound: {e}"
                );
                return Err(e);
            }
        }
    }
    Ok(out)
}

/// Fire all signed txs in the same millisecond. Hedge: spray nonce 0 to all 3 IPs.
pub async fn fire_burst(sub: &Submitter, txs: &[SignedTx], spray_first: bool) -> BurstResult {
    if txs.is_empty() {
        return BurstResult {
            class: BurstClass::NonceGap,
            hash: None,
            attempt: 0,
            message: "empty burst".into(),
            outcomes: Vec::new(),
        };
    }
    let t0 = Instant::now();
    let mut futs = Vec::new();
    for (i, tx) in txs.iter().enumerate() {
        if i == 0 && spray_first {
            futs.push(futures_util::future::Either::Left(async move {
                (0usize, sub.spray(&tx.raw).await)
            }));
        } else {
            let raw = tx.raw.clone();
            futs.push(futures_util::future::Either::Right(async move {
                (i, sub.send_raw(&raw).await)
            }));
        }
    }
    let results = futures_util::future::join_all(futs).await;
    let outcomes = results.clone();
    // Prefer an accepted hash over a later-indexed revert/dup: an accepted send
    // is the only outcome worth reconciling against a receipt.
    let mut first_revert: Option<(usize, SendOutcome)> = None;
    let mut first_known: Option<usize> = None;
    for (i, out) in results {
        match out {
            SendOutcome::Hash(h) => {
                return BurstResult {
                    class: BurstClass::Submitted,
                    hash: Some(h),
                    attempt: i,
                    message: format!("submitted attempt {i} in {} ms", t0.elapsed().as_millis()),
                    outcomes,
                };
            }
            SendOutcome::Revert { .. } => {
                if first_revert.is_none() {
                    first_revert = Some((i, out));
                }
            }
            SendOutcome::Known if first_known.is_none() => {
                first_known = Some(i);
            }
            _ => {}
        }
    }
    if let Some(i) = first_known {
        return BurstResult {
            class: BurstClass::LateDup,
            hash: Some(txs[i].hash),
            attempt: i,
            message: "already known".into(),
            outcomes,
        };
    }
    if let Some((i, SendOutcome::Revert { message })) = first_revert {
        return BurstResult {
            class: BurstClass::HelperRevert,
            hash: None,
            attempt: i,
            message,
            outcomes,
        };
    }
    BurstResult {
        class: BurstClass::NonceGap,
        hash: None,
        attempt: 0,
        message: "no inclusion".into(),
        outcomes,
    }
}

/// Sleep until `launched_at + entry_second` minus lead (in chain time), then
/// fire. `armed` is re-checked after the sleep so a pause during the wait
/// aborts before send.
pub async fn clock_fire(
    clock: &ChainClock,
    launched_at: u64,
    entry_second: u64,
    lead_ms: u64,
    sub: &Submitter,
    txs: &[SignedTx],
    armed: Option<&std::sync::atomic::AtomicBool>,
) -> BurstResult {
    let boundary = boundary_instant(launched_at, entry_second);
    let target = boundary.saturating_mul(1000).saturating_sub(lead_ms);
    let now_chain = clock.chain_now_ms();
    if (target as i64) > now_chain {
        tokio::time::sleep(std::time::Duration::from_millis(
            (target as i64 - now_chain) as u64,
        ))
        .await;
    }
    if is_paused(armed) || clock.require_quality(2_000, 1_500).is_err() {
        return BurstResult {
            class: BurstClass::Aborted,
            hash: None,
            attempt: 0,
            message: "paused or chain clock became unhealthy before send".into(),
            outcomes: Vec::new(),
        };
    }
    fire_burst(sub, txs, true).await
}

fn is_paused(flag: Option<&AtomicBool>) -> bool {
    flag.is_some_and(|f| f.load(Ordering::SeqCst))
}

/// Dry-run shadow sends nothing and leaves landing unmeasured.
pub fn shadow_result(launched_at: u64, entry_second: u64) -> BurstResult {
    BurstResult {
        class: BurstClass::ShadowWouldLand,
        hash: None,
        attempt: 0,
        message: format!(
            "dry run: would enter at unix {} (+{entry_second}); landing block unmeasured",
            launched_at + entry_second
        ),
        outcomes: Vec::new(),
    }
}

pub fn encode_buy_once(
    curve: Address,
    token: Address,
    recipient: Address,
    max_tax_bps: U256,
    min_tokens_out: U256,
    max_real_quote: U256,
) -> Bytes {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paused_flag_aborts_and_its_absence_permits() {
        assert!(is_paused(Some(&AtomicBool::new(true))));
        assert!(!is_paused(Some(&AtomicBool::new(false))));
        assert!(!is_paused(None));
    }
}
