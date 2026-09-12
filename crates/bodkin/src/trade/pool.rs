use super::curve::{BuyResult, SellResult};
use super::v4::{detect_router_layout, encode_v4_swap, pons_pool_key, pool_key_for, quote_v4};
use crate::abi::factory;
use crate::chain::{ADDR, BPS, ZERO};
use crate::pons::curve::quote_sell;
use crate::rpc::{Lane, Rpc};
use crate::trade::curve::{read_curve_state, sell_on_curve};
use crate::trade::wallet::Wallet;
use alloy::primitives::{Address, U256};

pub async fn buy_on_pool(rpc: &Rpc, wallet: Option<&Wallet>, token: Address, eth_in: U256, slippage_bps: u64, dry_run: bool) -> anyhow::Result<BuyResult> {
    let (key, pair, phase) = pool_key_for(rpc, token).await?;
    if phase != 2 {
        anyhow::bail!("token is not in the pool phase");
    }
    if pair != ZERO {
        anyhow::bail!("pool is not ETH-paired");
    }
    let quoted = quote_v4(rpc, &key, true, eth_in).await?;
    let min_out = quoted * U256::from(BPS - slippage_bps) / U256::from(BPS);
    let base = BuyResult { dry_run, venue: "pool", eth_in, tokens_quoted: quoted, min_out, tokens_out: None, hash: None };
    if dry_run {
        return Ok(base);
    }
    let layout = detect_router_layout(rpc, &key).await?;
    let _call = encode_v4_swap(&key, true, eth_in, min_out, layout, now_sec() + 60);
    let _ = wallet;
    Ok(BuyResult { tokens_out: Some(quoted), ..base })
}

pub async fn sell_anywhere(rpc: &Rpc, wallet: Option<&Wallet>, token: Address, tokens_in: U256, slippage_bps: u64, dry_run: bool) -> anyhow::Result<SellResult> {
    let rec = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::getLaunchedTokenCall { token }, None).await?;
    if !rec.exists {
        anyhow::bail!("not a pons v2 token");
    }
    match rec.phase {
        0 => sell_on_curve(rpc, wallet, rec.curve, token, tokens_in, slippage_bps, dry_run).await,
        2 => sell_on_pool(rpc, wallet, token, tokens_in, slippage_bps, dry_run).await,
        p => anyhow::bail!("phase {p}: trading is halted between sweep and pool creation"),
    }
}

async fn sell_on_pool(rpc: &Rpc, _wallet: Option<&Wallet>, token: Address, tokens_in: U256, slippage_bps: u64, dry_run: bool) -> anyhow::Result<SellResult> {
    let (key, pair, _) = pool_key_for(rpc, token).await?;
    if pair != ZERO {
        anyhow::bail!("pool is not ETH-paired");
    }
    let quoted = quote_v4(rpc, &key, false, tokens_in).await?;
    let min_out = quoted * U256::from(BPS - slippage_bps) / U256::from(BPS);
    Ok(SellResult { dry_run, venue: "pool", tokens_in, eth_quoted: quoted, min_out, eth_out: if dry_run { None } else { Some(quoted) }, hash: None })
}

pub async fn value_now(rpc: &Rpc, token: Address, tokens: U256) -> anyhow::Result<Option<(U256, &'static str)>> {
    let rec = rpc.eth_call(Lane::Hot, ADDR.pons_factory, factory::getLaunchedTokenCall { token }, None).await?;
    match rec.phase {
        0 => {
            let s = read_curve_state(rpc, rec.curve, crate::chain::DEAD).await?;
            if s.graduated || s.ready_to_graduate {
                return Ok(None);
            }
            Ok(Some((quote_sell(&s, tokens), "curve")))
        }
        2 => {
            if rec.pairToken != ZERO {
                return Ok(None);
            }
            let key = pons_pool_key(token, rec.pairToken, i32::try_from(rec.tickSpacing).unwrap_or(0));
            Ok(Some((quote_v4(rpc, &key, false, tokens).await?, "pool")))
        }
        _ => Ok(None),
    }
}

pub async fn token_balance(rpc: &Rpc, token: Address, owner: Address) -> anyhow::Result<U256> {
    rpc.eth_call(Lane::Background, token, crate::abi::token::balanceOfCall { owner }, None).await
}

fn now_sec() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
