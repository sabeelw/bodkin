use crate::abi::curve;
use crate::chain::{BPS, ZERO};
use crate::pons::curve::{CurveState, min_out_from_rate, quote_buy, quote_sell};
use crate::rpc::{Lane, Rpc};
use crate::trade::exec::{LiveExec, TxFinal};
use alloy::primitives::{Address, B256, U256};
use alloy::sol_types::{SolCall, SolEvent};

#[derive(Debug, Clone)]
pub struct BuyResult {
    pub venue: &'static str,
    pub eth_in: U256,
    pub tokens_quoted: U256,
    pub min_out: U256,
    /// Actual tokens received — only `Some` after a confirmed receipt.
    pub tokens_out: Option<U256>,
    /// Transaction hash — only `Some` after a confirmed receipt.
    pub hash: Option<B256>,
    pub gas_wei: U256,
}

#[derive(Debug, Clone)]
pub struct SellResult {
    pub venue: &'static str,
    pub tokens_in: U256,
    pub eth_quoted: U256,
    pub min_out: U256,
    /// Actual ETH out — only `Some` after a confirmed receipt.
    pub eth_out: Option<U256>,
    pub hash: Option<B256>,
    pub gas_wei: U256,
}

pub async fn read_curve_state(
    rpc: &Rpc,
    curve_addr: Address,
    recipient: Address,
) -> anyhow::Result<CurveState> {
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
        detected_at_ms: 0,
        source: "unknown",
    };
    let (_, _, curve, _) = crate::pons::enrich::read_launch_bundle(rpc, &ev, rec).await?;
    curve.ok_or_else(|| anyhow::anyhow!("curve state unreadable"))
}

pub async fn buy_on_curve(
    rpc: &Rpc,
    exec: Option<&LiveExec<'_>>,
    curve_addr: Address,
    eth_in: U256,
    slippage_bps: u64,
    dry_run: bool,
    state: Option<&CurveState>,
) -> anyhow::Result<BuyResult> {
    if slippage_bps >= BPS {
        anyhow::bail!("slippage {slippage_bps} bps is not < 10000");
    }
    let recipient = if dry_run {
        ZERO
    } else {
        exec.ok_or_else(|| anyhow::anyhow!("live curve buy needs a wallet + sequencer submitter"))?
            .wallet
            .address()
    };
    let s = match state {
        Some(s) => s.clone(),
        None => read_curve_state(rpc, curve_addr, recipient).await?,
    };
    let q = quote_buy(&s, eth_in);
    let min_out = min_out_from_rate(q.tokens_out, slippage_bps);
    let base = BuyResult {
        venue: "curve",
        eth_in: q.spent,
        tokens_quoted: q.tokens_out,
        min_out,
        tokens_out: None,
        hash: None,
        gas_wei: U256::ZERO,
    };
    if dry_run {
        return Ok(base);
    }
    let exec = exec.expect("checked above");
    let data = curve::buyCall {
        quoteIn: eth_in,
        minTokensOut: min_out,
        recipient,
    }
    .abi_encode();
    match exec
        .send_and_wait_tagged("curve_buy", curve_addr, eth_in, data.into(), None)
        .await?
    {
        TxFinal::Confirmed {
            hash,
            gas_used,
            effective_gas_price,
            logs,
            ..
        } => {
            let tokens = parse_curve_buy_tokens(&logs, curve_addr, recipient);
            if tokens.is_zero() {
                anyhow::bail!(
                    "buy {hash:#x} confirmed but emitted no CurveBuy for this wallet — inspect the receipt before retrying"
                );
            }
            let actual = eth_in.saturating_sub(parse_curve_refund(&logs, curve_addr, recipient));
            if actual.is_zero() {
                anyhow::bail!("buy {hash:#x} confirmed but moved no ETH into the curve");
            }
            Ok(BuyResult {
                eth_in: actual,
                tokens_out: Some(tokens),
                hash: Some(hash),
                gas_wei: gas_used.saturating_mul(effective_gas_price),
                ..base
            })
        }
        TxFinal::Reverted { hash, .. } => anyhow::bail!("buy reverted on-chain ({hash:#x})"),
        TxFinal::Unresolved { hash } => anyhow::bail!(
            "buy {hash:#x} was accepted by the sequencer but produced no receipt in time — check the wallet before retrying, it may still land"
        ),
    }
}

pub async fn sell_on_curve(
    rpc: &Rpc,
    exec: Option<&LiveExec<'_>>,
    curve_addr: Address,
    token: Address,
    tokens_in: U256,
    slippage_bps: u64,
    dry_run: bool,
) -> anyhow::Result<SellResult> {
    if slippage_bps >= BPS {
        anyhow::bail!("slippage {slippage_bps} bps is not < 10000");
    }
    let s = read_curve_state(rpc, curve_addr, crate::chain::DEAD).await?;
    if s.graduated || s.ready_to_graduate {
        anyhow::bail!("curve is closed (graduated or ready to graduate); sell on the pool instead");
    }
    let eth_quoted = quote_sell(&s, tokens_in);
    let min_out = min_out_from_rate(eth_quoted, slippage_bps);
    let base = SellResult {
        venue: "curve",
        tokens_in,
        eth_quoted,
        min_out,
        eth_out: None,
        hash: None,
        gas_wei: U256::ZERO,
    };
    if dry_run {
        return Ok(base);
    }
    let exec = exec
        .ok_or_else(|| anyhow::anyhow!("live curve sell needs a wallet + sequencer submitter"))?;
    let me = exec.wallet.address();
    let approval_gas = ensure_allowance(rpc, exec, token, curve_addr, tokens_in).await?;
    let data = curve::sellCall {
        tokensIn: tokens_in,
        minQuoteOut: min_out,
        recipient: me,
    }
    .abi_encode();
    match exec
        .send_and_wait_tagged("curve_sell", curve_addr, U256::ZERO, data.into(), None)
        .await?
    {
        TxFinal::Confirmed {
            hash,
            gas_used,
            effective_gas_price,
            logs,
            ..
        } => {
            let (tokens_in, eth_out) = parse_curve_sell_fill(&logs, curve_addr, me);
            if tokens_in.is_zero() || eth_out.is_zero() {
                anyhow::bail!(
                    "sell {hash:#x} confirmed but emitted no nonzero CurveSell fill for this wallet"
                );
            }
            Ok(SellResult {
                tokens_in,
                eth_out: Some(eth_out),
                hash: Some(hash),
                gas_wei: approval_gas.saturating_add(gas_used.saturating_mul(effective_gas_price)),
                ..base
            })
        }
        TxFinal::Reverted { hash, .. } => anyhow::bail!("sell reverted on-chain ({hash:#x})"),
        TxFinal::Unresolved { hash } => anyhow::bail!(
            "sell {hash:#x} was accepted by the sequencer but produced no receipt in time — position left open, check before retrying"
        ),
    }
}

/// Approve `spender` for `amount` if the current allowance is short. Sends a
/// real approve tx and waits for its receipt. Returns the confirmed gas cost.
pub async fn ensure_allowance(
    rpc: &Rpc,
    exec: &LiveExec<'_>,
    token: Address,
    spender: Address,
    amount: U256,
) -> anyhow::Result<U256> {
    let me = exec.wallet.address();
    let current: U256 = rpc
        .eth_call(
            Lane::Background,
            token,
            crate::abi::token::allowanceCall { owner: me, spender },
            None,
        )
        .await?;
    if current >= amount {
        return Ok(U256::ZERO);
    }
    let data = crate::abi::token::approveCall {
        spender,
        amount: U256::MAX,
    }
    .abi_encode();
    match exec
        .send_and_wait_tagged("token_approval", token, U256::ZERO, data.into(), None)
        .await?
    {
        TxFinal::Confirmed {
            gas_used,
            effective_gas_price,
            ..
        } => Ok(gas_used.saturating_mul(effective_gas_price)),
        TxFinal::Reverted { hash, .. } => anyhow::bail!("approve reverted on-chain ({hash:#x})"),
        TxFinal::Unresolved { hash } => {
            anyhow::bail!("approve {hash:#x} unresolved — refusing to continue the sequence")
        }
    }
}

/// Sum `CurveBuy.tokensOut` for `recipient` in this receipt's logs.
pub fn parse_curve_buy_tokens(
    logs: &[alloy::rpc::types::Log],
    curve_addr: Address,
    recipient: Address,
) -> U256 {
    let mut out = U256::ZERO;
    for l in logs {
        if l.address() != curve_addr {
            continue;
        }
        if let Ok(b) = curve::CurveBuy::decode_log(&l.clone().into())
            && (recipient == ZERO || b.recipient == recipient)
        {
            out = out.saturating_add(b.tokensOut);
        }
    }
    out
}

/// Sum `CurveSell.quoteOut` paid to `recipient` in this receipt's logs.
pub fn parse_curve_sell_fill(
    logs: &[alloy::rpc::types::Log],
    curve_addr: Address,
    recipient: Address,
) -> (U256, U256) {
    let mut tokens = U256::ZERO;
    let mut quote = U256::ZERO;
    for l in logs {
        if l.address() != curve_addr {
            continue;
        }
        if let Ok(s) = curve::CurveSell::decode_log(&l.clone().into())
            && (recipient == ZERO || s.recipient == recipient)
        {
            tokens = tokens.saturating_add(s.tokensIn);
            quote = quote.saturating_add(s.quoteOut);
        }
    }
    (tokens, quote)
}

pub fn parse_curve_sell_quote(
    logs: &[alloy::rpc::types::Log],
    curve_addr: Address,
    recipient: Address,
) -> U256 {
    parse_curve_sell_fill(logs, curve_addr, recipient).1
}

/// Sum `CurveBuyRefunded.refundAmount` for `recipient` — the helper returns
/// leftover ETH, so real spend is `sent − refunded`.
pub fn parse_curve_refund(
    logs: &[alloy::rpc::types::Log],
    curve_addr: Address,
    recipient: Address,
) -> U256 {
    let mut out = U256::ZERO;
    for l in logs {
        if l.address() != curve_addr {
            continue;
        }
        if let Ok(r) = curve::CurveBuyRefunded::decode_log(&l.clone().into())
            && (recipient == ZERO || r.recipient == recipient)
        {
            out = out.saturating_add(r.refundAmount);
        }
    }
    out
}
