use crate::abi::escrow;
use crate::chain::ADDR;
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;

#[derive(Debug, Clone)]
pub struct Claim {
    pub amount: U256,
    pub block: u64,
    pub timestamp: u64,
    pub tx: B256,
}

#[derive(Debug, Clone)]
pub struct FeeForensics {
    pub recipient: Address,
    pub curve_credited: U256,
    pub curve_credits: u32,
    pub pool_credited: U256,
    pub pool_credits: u32,
    pub total_credited: U256,
    pub claims: Vec<Claim>,
    pub total_claimed: U256,
    pub pending: U256,
}

pub async fn fee_forensics(
    rpc: &Rpc,
    recipient: Address,
    curve: Address,
    from_block: u64,
) -> anyhow::Result<FeeForensics> {
    let head = rpc.block_number(Lane::Background).await?;
    let step = 1_000_000u64;
    let mut out = FeeForensics {
        recipient,
        curve_credited: U256::ZERO,
        curve_credits: 0,
        pool_credited: U256::ZERO,
        pool_credits: 0,
        total_credited: U256::ZERO,
        claims: vec![],
        total_claimed: U256::ZERO,
        pending: U256::ZERO,
    };
    let mut b = from_block;
    while b <= head {
        let to = (b + step - 1).min(head);
        let credits = rpc
            .get_logs(
                Lane::Background,
                Filter::new()
                    .address(ADDR.pons_escrow)
                    .event_signature(escrow::Credited::SIGNATURE_HASH)
                    .topic1(alloy::primitives::B256::left_padding_from(
                        recipient.as_slice(),
                    ))
                    .from_block(b)
                    .to_block(to),
            )
            .await?;
        let claims = rpc
            .get_logs(
                Lane::Background,
                Filter::new()
                    .address(ADDR.pons_escrow)
                    .event_signature(escrow::Claimed::SIGNATURE_HASH)
                    .topic1(alloy::primitives::B256::left_padding_from(
                        recipient.as_slice(),
                    ))
                    .from_block(b)
                    .to_block(to),
            )
            .await?;
        for l in credits {
            if let Ok(c) = escrow::Credited::decode_log(&l.clone().into()) {
                if c.depositor == curve {
                    out.curve_credited += c.amount;
                    out.curve_credits += 1;
                } else {
                    out.pool_credited += c.amount;
                    out.pool_credits += 1;
                }
                out.total_credited += c.amount;
            }
        }
        for l in claims {
            if let Ok(c) = escrow::Claimed::decode_log(&l.clone().into()) {
                out.claims.push(Claim {
                    amount: c.amount,
                    block: l.block_number.unwrap_or(0),
                    timestamp: 0,
                    tx: l.transaction_hash.unwrap_or_default(),
                });
                out.total_claimed += c.amount;
            }
        }
        if to == head {
            break;
        }
        b = to + 1;
    }
    for c in out.claims.iter_mut().take(60) {
        if let Ok(ts) = rpc.block_timestamp(Lane::Background, c.block).await {
            c.timestamp = ts;
        }
    }
    out.pending = rpc
        .eth_call(
            Lane::Background,
            ADDR.pons_escrow,
            escrow::balanceOfCall { recipient },
            None,
        )
        .await
        .unwrap_or(U256::ZERO);
    Ok(out)
}
