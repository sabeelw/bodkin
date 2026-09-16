use super::scheduler::EntryDeadline;
use super::submitter::{SendOutcome, Submitter};
use super::wallet::Wallet;
use crate::pons::clock::ChainClock;
use alloy::primitives::{Address, B256, Bytes, U256};
use futures_util::stream::{FuturesUnordered, StreamExt};
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
    Expired,
    ShadowWouldLand,
}

#[derive(Debug, Clone)]
pub struct SignedTx {
    pub nonce: u64,
    pub hash: B256,
    pub raw: Bytes,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BurstTiming {
    pub first_response_ms: Option<u64>,
    pub first_useful_ms: Option<u64>,
    pub all_responses_ms: u64,
}

#[derive(Debug, Clone)]
pub struct BurstResult {
    pub class: BurstClass,
    pub hash: Option<B256>,
    pub attempt: usize,
    pub message: String,
    pub outcomes: Vec<(usize, SendOutcome)>,
    pub timing: BurstTiming,
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
            timing: BurstTiming::default(),
        };
    }
    let t0 = Instant::now();
    let mut futures = FuturesUnordered::new();
    for (index, tx) in txs.iter().enumerate() {
        let raw = tx.raw.clone();
        futures.push(async move {
            let outcome = if index == 0 && spray_first {
                sub.spray(&raw).await
            } else {
                sub.send_raw(&raw).await
            };
            (index, outcome)
        });
    }
    let mut outcomes = Vec::with_capacity(txs.len());
    let mut timing = BurstTiming::default();
    while let Some((index, outcome)) = futures.next().await {
        let elapsed = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
        timing.first_response_ms.get_or_insert(elapsed);
        if !matches!(outcome, SendOutcome::Error(_)) {
            timing.first_useful_ms.get_or_insert(elapsed);
        }
        outcomes.push((index, outcome));
    }
    timing.all_responses_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
    outcomes.sort_by_key(|(index, _)| *index);
    // Prefer an accepted hash over a later-indexed revert/dup: an accepted send
    // is the only outcome worth reconciling against a receipt.
    let mut first_revert: Option<(usize, SendOutcome)> = None;
    let mut first_known: Option<usize> = None;
    for (index, outcome) in outcomes.iter().cloned() {
        match outcome {
            SendOutcome::Hash(hash) => {
                return BurstResult {
                    class: BurstClass::Submitted,
                    hash: Some(hash),
                    attempt: index,
                    message: format!(
                        "submitted attempt {index} in {} ms",
                        timing.all_responses_ms
                    ),
                    outcomes,
                    timing,
                };
            }
            SendOutcome::Revert { .. } => {
                if first_revert.is_none() {
                    first_revert = Some((index, outcome));
                }
            }
            SendOutcome::Known if first_known.is_none() => {
                first_known = Some(index);
            }
            _ => {}
        }
    }
    if let Some(index) = first_known {
        return BurstResult {
            class: BurstClass::LateDup,
            hash: Some(txs[index].hash),
            attempt: index,
            message: "already known".into(),
            outcomes,
            timing,
        };
    }
    if let Some((index, SendOutcome::Revert { message })) = first_revert {
        return BurstResult {
            class: BurstClass::HelperRevert,
            hash: None,
            attempt: index,
            message,
            outcomes,
            timing,
        };
    }
    BurstResult {
        class: BurstClass::NonceGap,
        hash: None,
        attempt: 0,
        message: "no inclusion".into(),
        outcomes,
        timing,
    }
}

/// Sleep until the typed entry deadline's dispatch instant, then fire.
/// `armed` is re-checked after the sleep so a pause during the wait aborts
/// before send.
pub async fn clock_fire(
    clock: &ChainClock,
    deadline: EntryDeadline,
    sub: &Submitter,
    txs: &[SignedTx],
    armed: Option<&AtomicBool>,
) -> BurstResult {
    if !deadline.preparation_open(clock.chain_now_ms()) {
        return empty_result(
            BurstClass::Expired,
            "entry preparation missed dispatch cutoff",
        );
    }
    let target = match deadline.dispatch_instant(clock) {
        Ok(target) => target,
        Err(error) => return empty_result(BurstClass::Expired, error.to_string()),
    };
    tokio::time::sleep_until(tokio::time::Instant::from_std(target)).await;
    if is_paused(armed) || clock.require_quality(2_000, 1_500).is_err() {
        return empty_result(
            BurstClass::Aborted,
            "paused or chain clock became unhealthy before send",
        );
    }
    if !deadline.dispatch_open(clock.chain_now_ms()) {
        return empty_result(
            BurstClass::Expired,
            "entry scheduler missed dispatch tolerance",
        );
    }
    fire_burst(sub, txs, true).await
}

fn empty_result(class: BurstClass, message: impl Into<String>) -> BurstResult {
    BurstResult {
        class,
        hash: None,
        attempt: 0,
        message: message.into(),
        outcomes: Vec::new(),
        timing: BurstTiming::default(),
    }
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
        timing: BurstTiming::default(),
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
