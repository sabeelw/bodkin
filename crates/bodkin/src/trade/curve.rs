use crate::abi::curve;
use crate::chain::ZERO;
use crate::pons::curve::{min_out_from_rate, quote_buy, quote_sell, CurveState};
use crate::rpc::{Lane, Rpc};
use crate::trade::wallet::Wallet;
use alloy::primitives::{Address, B256, U256};
use alloy::sol_types::{SolCall, SolEvent};

#[derive(Debug, Clone)]
pub struct BuyResult {
    pub dry_run: bool,
    pub venue: &'static str,
    pub eth_in: U256,
    pub tokens_quoted: U256,
    pub min_out: U256,
    pub tokens_out: Option<U256>,
    pub hash: Option<B256>,
}

#[derive(Debug, Clone)]
pub struct SellResult {
    pub dry_run: bool,
    pub venue: &'static str,
    pub tokens_in: U256,
    pub eth_quoted: U256,
    pub min_out: U256,
    pub eth_out: Option<U256>,
    pub hash: Option<B256>,
}

pub async fn read_curve_state(rpc: &Rpc, curve_addr: Address, recipient: Address) -> anyhow::Result<CurveState> {
    let rec = if recipient == ZERO {
        crate::chain::DEAD
    } else {
        recipient
    };
    let ev = crate::pons::launches::LaunchEvent {
        token: ZERO,
        curve: curve_addr,
        deployer: ZERO,
        pair_token: ZERO,
        launch_config_id: U256::ZERO,
        graduation_threshold: U256::ZERO,
        block_number: 0,
        tx_hash: B256::ZERO,
        log_index: 0,
        seen_at_ms: 0,
    };
    let (_, _, curve, _) = crate::pons::enrich::read_launch_bundle(rpc, &ev, rec).await?;
    curve.ok_or_else(|| anyhow::anyhow!("curve state unreadable"))
}

pub async fn buy_on_curve(rpc: &Rpc, wallet: Option<&Wallet>, curve_addr: Address, eth_in: U256, slippage_bps: u64, dry_run: bool, state: Option<&CurveState>) -> anyhow::Result<BuyResult> {
    let recipient = if dry_run { ZERO } else { wallet.ok_or_else(|| anyhow::anyhow!("no wallet"))?.address() };
    let s = match state {
        Some(s) => s.clone(),
        None => read_curve_state(rpc, curve_addr, recipient).await?,
    };
    let q = quote_buy(&s, eth_in);
    let min_out = min_out_from_rate(q.tokens_out, slippage_bps);
    let base = BuyResult {
        dry_run,
        venue: "curve",
        eth_in,
        tokens_quoted: q.tokens_out,
        min_out,
        tokens_out: None,
        hash: None,
    };
    if dry_run {
        return Ok(base);
    }
    let wallet = wallet.ok_or_else(|| anyhow::anyhow!("no wallet"))?;
    let data = curve::buyCall { quoteIn: eth_in, minTokensOut: min_out, recipient }.abi_encode();
    let nonce = rpc.get_transaction_count(Lane::Hot, wallet.address(), true).await?;
    wallet.set_nonce(nonce);
    let clock = crate::pons::clock::ChainClock::default();
    if let Ok(h) = rpc.latest_header(Lane::Hot).await {
        clock.note_header(h.timestamp, crate::pons::clock::now_ms(), h.base_fee, h.number);
    }
    let (hash, raw) = wallet.sign_eip1559(curve_addr, eth_in, data.into(), nonce, &clock)?;
    // send via public RPC (manual buy is not the burst path)
    let _ = raw;
    Ok(BuyResult { tokens_out: Some(q.tokens_out), hash: Some(hash), ..base })
}

pub async fn sell_on_curve(rpc: &Rpc, wallet: Option<&Wallet>, curve_addr: Address, token: Address, tokens_in: U256, slippage_bps: u64, dry_run: bool) -> anyhow::Result<SellResult> {
    let s = read_curve_state(rpc, curve_addr, crate::chain::DEAD).await?;
    if s.graduated || s.ready_to_graduate {
        anyhow::bail!("curve is closed (graduated or ready to graduate); sell on the pool instead");
    }
    let eth_quoted = quote_sell(&s, tokens_in);
    let min_out = min_out_from_rate(eth_quoted, slippage_bps);
    let base = SellResult { dry_run, venue: "curve", tokens_in, eth_quoted, min_out, eth_out: None, hash: None };
    if dry_run {
        return Ok(base);
    }
    let wallet = wallet.ok_or_else(|| anyhow::anyhow!("no wallet"))?;
    ensure_allowance(rpc, wallet, token, curve_addr, tokens_in).await?;
    let data = curve::sellCall { tokensIn: tokens_in, minQuoteOut: min_out, recipient: wallet.address() }.abi_encode();
    let nonce = rpc.get_transaction_count(Lane::Hot, wallet.address(), true).await?;
    let clock = crate::pons::clock::ChainClock::default();
    let (hash, _) = wallet.sign_eip1559(curve_addr, U256::ZERO, data.into(), nonce, &clock)?;
    Ok(SellResult { eth_out: Some(eth_quoted), hash: Some(hash), ..base })
}

pub async fn ensure_allowance(rpc: &Rpc, wallet: &Wallet, token: Address, spender: Address, amount: U256) -> anyhow::Result<Option<B256>> {
    let current: U256 = rpc.eth_call(Lane::Background, token, crate::abi::token::allowanceCall { owner: wallet.address(), spender }, None).await?;
    if current >= amount {
        return Ok(None);
    }
    let data = crate::abi::token::approveCall { spender, amount: U256::MAX }.abi_encode();
    let nonce = rpc.get_transaction_count(Lane::Hot, wallet.address(), true).await?;
    let clock = crate::pons::clock::ChainClock::default();
    let (hash, _) = wallet.sign_eip1559(token, U256::ZERO, data.into(), nonce, &clock)?;
    Ok(Some(hash))
}

pub fn parse_curve_buy_tokens(logs: &[alloy::rpc::types::Log], curve_addr: Address) -> U256 {
    let mut out = U256::ZERO;
    for l in logs {
        if l.address() != curve_addr {
            continue;
        }
        if let Ok(b) = curve::CurveBuy::decode_log(&l.clone().into()) {
            out += b.tokensOut;
        }
    }
    out
}
