use super::curve::{BuyResult, SellResult, ensure_allowance};
use super::v4::{
    PoolKey, detect_router_layout, encode_v4_swap, pons_pool_key, pool_id, pool_key_for, quote_v4,
};
use crate::abi::{curve, factory, permit2, poolManager};
use crate::chain::{ADDR, BPS, ZERO};
use crate::pons::curve::{progress, quote_sell};
use crate::rpc::{Lane, Rpc};
use crate::trade::curve::{read_curve_state, sell_on_curve};
use crate::trade::exec::{LiveExec, TxFinal};
use alloy::primitives::{Address, U256};
use alloy::sol_types::{SolCall, SolEvent};

/// v4 router swaps are heavier than the curve helper — 350k is tight.
const V4_GAS: u64 = 700_000;

pub async fn buy_on_pool(
    rpc: &Rpc,
    exec: Option<&LiveExec<'_>>,
    token: Address,
    eth_in: U256,
    slippage_bps: u64,
    dry_run: bool,
) -> anyhow::Result<BuyResult> {
    if slippage_bps >= BPS {
        anyhow::bail!("slippage {slippage_bps} bps is not < 10000");
    }
    let (key, pair, phase) = pool_key_for(rpc, token).await?;
    if phase != 2 {
        anyhow::bail!("token is not in the pool phase");
    }
    if pair != ZERO {
        anyhow::bail!("pool is not ETH-paired");
    }
    let quoted = quote_v4(rpc, &key, true, eth_in).await?;
    let min_out = quoted * U256::from(BPS - slippage_bps) / U256::from(BPS);
    let base = BuyResult {
        venue: "pool",
        eth_in,
        tokens_quoted: quoted,
        min_out,
        tokens_out: None,
        hash: None,
        gas_wei: U256::ZERO,
    };
    if dry_run {
        return Ok(base);
    }
    let exec =
        exec.ok_or_else(|| anyhow::anyhow!("live pool buy needs a wallet + sequencer submitter"))?;
    let before = token_balance(rpc, token, exec.wallet.address()).await?;
    let layout = detect_router_layout(rpc, &key).await?;
    let call = encode_v4_swap(&key, true, eth_in, min_out, layout, now_sec() + 60)?;
    match exec
        .send_and_wait_tagged("pool_buy", call.to, call.value, call.data, Some(V4_GAS))
        .await?
    {
        TxFinal::Confirmed {
            hash,
            gas_used,
            effective_gas_price,
            ..
        } => {
            let after = token_balance(rpc, token, exec.wallet.address()).await?;
            let got = after.saturating_sub(before);
            if got.is_zero() {
                anyhow::bail!(
                    "pool buy {hash:#x} confirmed but the wallet gained no tokens — inspect the receipt before retrying"
                );
            }
            Ok(BuyResult {
                tokens_out: Some(got),
                hash: Some(hash),
                gas_wei: gas_used.saturating_mul(effective_gas_price),
                ..base
            })
        }
        TxFinal::Reverted { hash, .. } => anyhow::bail!("pool buy reverted on-chain ({hash:#x})"),
        TxFinal::Unresolved { hash } => anyhow::bail!(
            "pool buy {hash:#x} accepted but no receipt in time — check the wallet before retrying, it may still land"
        ),
    }
}

pub async fn sell_anywhere(
    rpc: &Rpc,
    exec: Option<&LiveExec<'_>>,
    token: Address,
    tokens_in: U256,
    slippage_bps: u64,
    dry_run: bool,
) -> anyhow::Result<SellResult> {
    let rec = rpc
        .eth_call(
            Lane::Enrich,
            ADDR.pons_factory,
            factory::getLaunchedTokenCall { token },
            None,
        )
        .await?;
    if !rec.exists {
        anyhow::bail!("not a pons v2 token");
    }
    match rec.phase {
        0 => {
            sell_on_curve(
                rpc,
                exec,
                rec.curve,
                token,
                tokens_in,
                slippage_bps,
                dry_run,
            )
            .await
        }
        2 => {
            let key = pons_pool_key(
                token,
                rec.pairToken,
                i32::try_from(rec.tickSpacing).unwrap_or(0),
            );
            sell_on_pool(
                rpc,
                exec,
                token,
                &key,
                rec.curve,
                rec.creatorTaxBps,
                tokens_in,
                slippage_bps,
                dry_run,
            )
            .await
        }
        p => anyhow::bail!("phase {p}: trading is halted between sweep and pool creation"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn sell_on_pool(
    rpc: &Rpc,
    exec: Option<&LiveExec<'_>>,
    token: Address,
    key: &PoolKey,
    curve_addr: Address,
    creator_tax_bps: u16,
    tokens_in: U256,
    slippage_bps: u64,
    dry_run: bool,
) -> anyhow::Result<SellResult> {
    if slippage_bps >= BPS {
        anyhow::bail!("slippage {slippage_bps} bps is not < 10000");
    }
    if key.currency0 != ZERO {
        anyhow::bail!("pool is not ETH-paired");
    }
    let quoted = quote_v4(rpc, key, false, tokens_in).await?;
    let min_out = quoted * U256::from(BPS - slippage_bps) / U256::from(BPS);
    let base = SellResult {
        venue: "pool",
        tokens_in,
        eth_quoted: quoted,
        min_out,
        eth_out: None,
        hash: None,
        gas_wei: U256::ZERO,
    };
    if dry_run {
        return Ok(base);
    }
    let exec =
        exec.ok_or_else(|| anyhow::anyhow!("live pool sell needs a wallet + sequencer submitter"))?;

    // The router pulls the ERC20 input through Permit2: token→permit2
    // allowance first (reusable), then a bounded permit2 approval for this
    // amount with a short expiry.
    let approval_gas = ensure_allowance(rpc, exec, token, ADDR.permit2, tokens_in)
        .await?
        .saturating_add(ensure_permit2(rpc, exec, token, ADDR.universal_router, tokens_in).await?);

    let layout = detect_router_layout(rpc, key).await?;
    let call = encode_v4_swap(key, false, tokens_in, min_out, layout, now_sec() + 60)?;
    match exec
        .send_and_wait_tagged("pool_sell", call.to, call.value, call.data, Some(V4_GAS))
        .await?
    {
        TxFinal::Confirmed {
            hash,
            gas_used,
            effective_gas_price,
            logs,
            ..
        } => {
            let (actual_tokens, gross_out) = parse_pool_sell_fill(&logs, key).ok_or_else(|| {
                anyhow::anyhow!(
                    "pool sell {hash:#x} confirmed but emitted no matching nonzero Swap fill"
                )
            })?;
            let eth_out = net_pool_quote(rpc, curve_addr, creator_tax_bps, gross_out).await?;
            if eth_out.is_zero() {
                anyhow::bail!("pool sell {hash:#x} confirmed but netted zero ETH after hook fees");
            }
            Ok(SellResult {
                tokens_in: actual_tokens,
                eth_out: Some(eth_out),
                hash: Some(hash),
                gas_wei: approval_gas.saturating_add(gas_used.saturating_mul(effective_gas_price)),
                ..base
            })
        }
        TxFinal::Reverted { hash, .. } => anyhow::bail!("pool sell reverted on-chain ({hash:#x})"),
        TxFinal::Unresolved { hash } => anyhow::bail!(
            "pool sell {hash:#x} accepted but no receipt in time — position left open, check before retrying"
        ),
    }
}

pub fn parse_pool_sell_fill(
    logs: &[alloy::rpc::types::Log],
    key: &PoolKey,
) -> Option<(U256, U256)> {
    let id = pool_id(key);
    let mut tokens_in = U256::ZERO;
    let mut gross_out = U256::ZERO;
    for log in logs {
        if log.address() != ADDR.v4_pool_manager {
            continue;
        }
        let Ok(swap) = poolManager::Swap::decode_log(&log.clone().into()) else {
            continue;
        };
        if swap.id == id && swap.amount0.is_positive() && swap.amount1.is_negative() {
            gross_out = gross_out.saturating_add(U256::from(swap.amount0.unsigned_abs()));
            tokens_in = tokens_in.saturating_add(U256::from(swap.amount1.unsigned_abs()));
        }
    }
    (!tokens_in.is_zero() && !gross_out.is_zero()).then_some((tokens_in, gross_out))
}

pub async fn net_pool_quote(
    rpc: &Rpc,
    curve_addr: Address,
    creator_tax_bps: u16,
    gross_out: U256,
) -> anyhow::Result<U256> {
    let fee_bps: U256 = rpc
        .eth_call(Lane::Hot, curve_addr, curve::feeBpsCall {}, None)
        .await?;
    let fee_bps: u64 = fee_bps
        .try_into()
        .map_err(|_| anyhow::anyhow!("curve fee bps does not fit u64"))?;
    Ok(gross_out
        .saturating_sub(bps_amount(gross_out, fee_bps))
        .saturating_sub(bps_amount(gross_out, creator_tax_bps as u64)))
}

fn bps_amount(value: U256, bps: u64) -> U256 {
    (value.widening_mul(U256::from(bps)) / alloy::primitives::U512::from(BPS)).to::<U256>()
}

/// Ensure `permit2` lets `spender` move `amount` of `token` for the wallet.
/// Re-approves with the exact amount and a 24h expiry when the current
/// allowance is short or expired.
async fn ensure_permit2(
    rpc: &Rpc,
    exec: &LiveExec<'_>,
    token: Address,
    spender: Address,
    amount: U256,
) -> anyhow::Result<U256> {
    let me = exec.wallet.address();
    let now = now_sec();
    let cur = rpc
        .eth_call(
            Lane::Background,
            ADDR.permit2,
            permit2::allowanceCall {
                user: me,
                token,
                spender,
            },
            None,
        )
        .await?;
    let need: u128 = amount
        .try_into()
        .map_err(|_| anyhow::anyhow!("amount does not fit u160 permit bound"))?;
    let has = u128::try_from(cur.amount).unwrap_or(0);
    let exp: u64 = u64::try_from(cur.expiration).unwrap_or(0);
    if has >= need && exp > now + 60 {
        return Ok(U256::ZERO);
    }
    let amount160: alloy::primitives::aliases::U160 = need
        .try_into()
        .map_err(|_| anyhow::anyhow!("amount exceeds u160"))?;
    let exp48: alloy::primitives::aliases::U48 = (now + 86_400)
        .try_into()
        .map_err(|_| anyhow::anyhow!("expiry overflow"))?;
    let data = permit2::approveCall {
        token,
        spender,
        amount: amount160,
        expiration: exp48,
    }
    .abi_encode();
    match exec
        .send_and_wait_tagged(
            "permit2_approval",
            ADDR.permit2,
            U256::ZERO,
            data.into(),
            None,
        )
        .await?
    {
        TxFinal::Confirmed {
            gas_used,
            effective_gas_price,
            ..
        } => Ok(gas_used.saturating_mul(effective_gas_price)),
        TxFinal::Reverted { hash, .. } => anyhow::bail!("permit2 approve reverted ({hash:#x})"),
        TxFinal::Unresolved { hash } => anyhow::bail!(
            "permit2 approve {hash:#x} unresolved — refusing to continue the sequence"
        ),
    }
}

/// What the position is worth if sold right now, plus the curve's real
/// progress toward graduation (0.0 on the pool, where graduation already
/// happened).
#[derive(Debug, Clone)]
pub struct Valuation {
    pub eth: U256,
    pub venue: &'static str,
    pub progress: f64,
    pub phase: u8,
}

pub async fn value_now(
    rpc: &Rpc,
    token: Address,
    tokens: U256,
) -> anyhow::Result<Option<Valuation>> {
    let rec = rpc
        .eth_call(
            Lane::Hot,
            ADDR.pons_factory,
            factory::getLaunchedTokenCall { token },
            None,
        )
        .await?;
    match rec.phase {
        0 => {
            let s = read_curve_state(rpc, rec.curve, crate::chain::DEAD).await?;
            if s.graduated || s.ready_to_graduate {
                return Ok(None);
            }
            Ok(Some(Valuation {
                eth: quote_sell(&s, tokens),
                venue: "curve",
                progress: progress(&s),
                phase: 0,
            }))
        }
        2 => {
            if rec.pairToken != ZERO {
                return Ok(None);
            }
            let key = pons_pool_key(
                token,
                rec.pairToken,
                i32::try_from(rec.tickSpacing).unwrap_or(0),
            );
            Ok(Some(Valuation {
                eth: quote_v4(rpc, &key, false, tokens).await?,
                venue: "pool",
                progress: 1.0,
                phase: 2,
            }))
        }
        _ => Ok(None),
    }
}

pub async fn token_balance(rpc: &Rpc, token: Address, owner: Address) -> anyhow::Result<U256> {
    rpc.eth_call(
        Lane::Background,
        token,
        crate::abi::token::balanceOfCall { owner },
        None,
    )
    .await
}

fn now_sec() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_sell_fill_uses_matching_positive_eth_delta() {
        let key = pons_pool_key(Address::from([1u8; 20]), Address::ZERO, 60);
        let event = poolManager::Swap {
            id: pool_id(&key),
            sender: Address::from([2u8; 20]),
            amount0: 900,
            amount1: -1_000,
            sqrtPriceX96: Default::default(),
            liquidity: 1,
            tick: Default::default(),
            fee: Default::default(),
        };
        let log = alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: ADDR.v4_pool_manager,
                data: event.encode_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        };
        assert_eq!(
            parse_pool_sell_fill(&[log], &key),
            Some((U256::from(1_000), U256::from(900)))
        );
    }
}
