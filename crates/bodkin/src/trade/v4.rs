use crate::abi::{factory, stateView, universalRouter, v4Quoter};
use crate::chain::{ADDR, ZERO};
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{
    aliases::{I24, U24},
    Address, B256, Bytes, U256,
};
use alloy::sol_types::{SolCall, SolValue};

#[derive(Debug, Clone, Copy)]
pub struct PoolKey {
    pub currency0: Address,
    pub currency1: Address,
    pub fee: u32,
    pub tick_spacing: i32,
    pub hooks: Address,
}

pub fn pons_pool_key(token: Address, pair_token: Address, tick_spacing: i32) -> PoolKey {
    let (c0, c1) = if token < pair_token { (token, pair_token) } else { (pair_token, token) };
    PoolKey { currency0: c0, currency1: c1, fee: 0, tick_spacing, hooks: ADDR.pons_hook }
}

pub fn pool_id(k: &PoolKey) -> B256 {
    let pk = v4Quoter::PoolKey {
        currency0: k.currency0,
        currency1: k.currency1,
        fee: u24(k.fee),
        tickSpacing: i24(k.tick_spacing),
        hooks: k.hooks,
    };
    alloy::primitives::keccak256(pk.abi_encode())
}

pub async fn pool_key_for(rpc: &Rpc, token: Address) -> anyhow::Result<(PoolKey, Address, u8)> {
    let r = rpc.eth_call(Lane::Enrich, ADDR.pons_factory, factory::getLaunchedTokenCall { token }, None).await?;
    if !r.exists {
        anyhow::bail!("not a pons v2 token");
    }
    Ok((pons_pool_key(token, r.pairToken, i32::try_from(r.tickSpacing).unwrap_or(0)), r.pairToken, r.phase))
}

#[derive(Debug, Clone)]
pub struct PoolState {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
}

pub async fn pool_state(rpc: &Rpc, key: &PoolKey) -> anyhow::Result<PoolState> {
    let id = pool_id(key);
    let slot = rpc.eth_call(Lane::Background, ADDR.v4_state_view, stateView::getSlot0Call { poolId: id }, None).await?;
    let liq = rpc.eth_call(Lane::Background, ADDR.v4_state_view, stateView::getLiquidityCall { poolId: id }, None).await?;
    Ok(PoolState {
        sqrt_price_x96: U256::from(slot.sqrtPriceX96),
        tick: i32::try_from(slot.tick).unwrap_or(0),
        liquidity: u128::try_from(liq).unwrap_or(0),
    })
}

pub async fn quote_v4(rpc: &Rpc, key: &PoolKey, zero_for_one: bool, amount_in: U256) -> anyhow::Result<U256> {
    let params = v4Quoter::QuoteExactSingleParams {
        poolKey: v4_key(key),
        zeroForOne: zero_for_one,
        exactAmount: amount_in.try_into().unwrap_or(0),
        hookData: Bytes::new(),
    };
    let ret = rpc.eth_call(Lane::Hot, ADDR.v4_quoter, v4Quoter::quoteExactInputSingleCall { params }, None).await?;
    Ok(ret.amountOut)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterLayout {
    Current,
    Legacy,
}

pub struct SwapCall {
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
}

const CMD_V4_SWAP: u8 = 0x10;
const ACT_SWAP: u8 = 0x06;
const ACT_SETTLE: u8 = 0x0c;
const ACT_TAKE: u8 = 0x0f;

pub fn encode_v4_swap(key: &PoolKey, zero_for_one: bool, amount_in: U256, amount_out_min: U256, layout: RouterLayout, deadline: u64) -> SwapCall {
    let actions = Bytes::from(vec![ACT_SWAP, ACT_SETTLE, ACT_TAKE]);
    let pk = v4_key(key);
    let swap = match layout {
        RouterLayout::Current => (pk.clone(), zero_for_one, u128_of(amount_in), u128_of(amount_out_min), U256::ZERO, Bytes::new()).abi_encode(),
        RouterLayout::Legacy => (pk, zero_for_one, u128_of(amount_in), u128_of(amount_out_min), Bytes::new()).abi_encode(),
    };
    let c_in = if zero_for_one { key.currency0 } else { key.currency1 };
    let c_out = if zero_for_one { key.currency1 } else { key.currency0 };
    let settle = (c_in, amount_in).abi_encode();
    let take = (c_out, amount_out_min).abi_encode();
    let input = (actions, vec![Bytes::from(swap), Bytes::from(settle), Bytes::from(take)]).abi_encode();
    let commands = Bytes::from(vec![CMD_V4_SWAP]);
    let data = Bytes::from(universalRouter::executeCall { commands, inputs: vec![Bytes::from(input)], deadline: U256::from(deadline) }.abi_encode());
    SwapCall { to: ADDR.universal_router, data, value: if c_in == ZERO { amount_in } else { U256::ZERO } }
}

fn u128_of(v: U256) -> u128 {
    v.try_into().unwrap_or(u128::MAX)
}

fn u24(n: u32) -> U24 {
    U24::from(n as u16)
}

fn i24(n: i32) -> I24 {
    I24::try_from(n).unwrap_or_default()
}

fn v4_key(key: &PoolKey) -> v4Quoter::PoolKey {
    v4Quoter::PoolKey {
        currency0: key.currency0,
        currency1: key.currency1,
        fee: u24(key.fee),
        tickSpacing: i24(key.tick_spacing),
        hooks: key.hooks,
    }
}

pub async fn detect_router_layout(rpc: &Rpc, key: &PoolKey) -> anyhow::Result<RouterLayout> {
    for layout in [RouterLayout::Current, RouterLayout::Legacy] {
        let call = encode_v4_swap(key, true, U256::from(100_000_000_000_000u64), U256::ZERO, layout, 1);
        if rpc.eth_call_raw(Lane::Background, call.to, &call.data).await.is_ok() {
            return Ok(layout);
        }
    }
    anyhow::bail!("neither UniversalRouter param layout simulates")
}
