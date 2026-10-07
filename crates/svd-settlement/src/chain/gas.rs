//! The gas strategy of the settlement submitter: pure EIP-1559 math over
//! [`GasConfig`], no I/O and no clock.
//!
//! The submitter prices a batch from the node's view of the chain
//! (`eth_getBlockByNumber`'s base fee and `eth_maxPriorityFeePerGas`) and
//! re-prices it on every replacement attempt. Both steps saturate instead of
//! overflowing: a saturated fee still produces a valid transaction, a
//! wrapping one could produce an underpriced one.

use crate::config::GasConfig;

/// Wei in one gwei.
const WEI_PER_GWEI: u128 = 1_000_000_000;
/// Basis points in one unit.
const BPS: u128 = 10_000;
/// Percent in one unit.
const PERCENT: u128 = 100;

/// The cap on the compounded bump steps of [`bump`].
///
/// The submitter's attempt counter is small in practice; the cap only
/// bounds the work of a pathological one. It is high enough that wei-scale
/// fees saturate instead of being truncated: the compounding loses at most
/// one wei per step, and a one-gwei value reaches `u128::MAX` within about
/// 6_900 steps at the smallest possible bump of one percent.
const MAX_BUMP_STEPS: u32 = 12_800;

/// The EIP-1559 price of one transaction: `(max_fee_per_gas,
/// max_priority_fee_per_gas)`, in wei.
///
/// The tip is the node's suggestion capped at `max_priority_fee_gwei`; the
/// fee cap is the current base fee plus the `base_fee_tolerance_bps`
/// headroom plus the tip, so the transaction stays includable while the base
/// fee drifts for a few blocks.
pub fn price_eip1559(current_base_fee: u128, suggested_tip: u128, cfg: &GasConfig) -> (u128, u128) {
    let tip = suggested_tip.min(tip_cap(cfg));
    let headroom = current_base_fee.saturating_add(
        current_base_fee.saturating_mul(u128::from(cfg.base_fee_tolerance_bps)) / BPS,
    );
    (headroom.saturating_add(tip), tip)
}

/// The replacement price of an attempt: both fees compounded `attempt` times
/// by `bump_pct`, the tip re-capped at `max_priority_fee_gwei`, the fee cap
/// never below `prev_max_fee`.
///
/// `attempt` is 1-based: the first replacement (attempt 1) applies one bump.
/// The fee cap is also floored at the (re-capped) tip — an EIP-1559
/// transaction with `max_fee < max_priority_fee` is rejected by the nodes.
pub fn bump(prev_max_fee: u128, prev_tip: u128, attempt: u32, cfg: &GasConfig) -> (u128, u128) {
    let mut max_fee = prev_max_fee;
    let mut tip = prev_tip;
    let factor = PERCENT.saturating_add(u128::from(cfg.bump_pct));
    for _ in 0..attempt.min(MAX_BUMP_STEPS) {
        max_fee = scale(max_fee, factor);
        tip = scale(tip, factor);
    }
    let tip = tip.min(tip_cap(cfg));
    (max_fee.max(prev_max_fee).max(tip), tip)
}

/// The gas limit of one transaction: the estimate plus the
/// `gas_buffer_pct` headroom, capped at `gas_limit_cap`, in gas units.
pub fn gas_limit(estimate: u64, cfg: &GasConfig) -> u64 {
    let buffered = u128::from(estimate)
        .saturating_mul(PERCENT.saturating_add(u128::from(cfg.gas_buffer_pct)))
        / PERCENT;
    buffered.min(u128::from(cfg.gas_limit_cap)) as u64
}

/// The absolute priority-fee cap from the config, in wei.
fn tip_cap(cfg: &GasConfig) -> u128 {
    cfg.max_priority_fee_gwei.saturating_mul(WEI_PER_GWEI)
}

/// Multiplies `value` by `factor` percent, saturating instead of wrapping.
///
/// The whole and fractional parts are scaled separately so a value near
/// `u128::MAX` saturates at the true mathematical limit; scaling first and
/// dividing after would overflow into a *smaller* number.
fn scale(value: u128, factor: u128) -> u128 {
    let whole = value / PERCENT;
    let frac = value % PERCENT;
    whole.saturating_mul(factor).saturating_add(frac.saturating_mul(factor) / PERCENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One gwei in wei.
    const GWEI: u128 = WEI_PER_GWEI;

    /// The default-like config: 2 gwei tip cap, 15% bump, 20 bps of base
    /// fee headroom, 30M gas cap, 10% gas buffer.
    fn config() -> GasConfig {
        GasConfig {
            max_priority_fee_gwei: 2,
            bump_pct: 15,
            base_fee_tolerance_bps: 20,
            gas_limit_cap: 30_000_000,
            gas_buffer_pct: 10,
        }
    }

    #[test]
    fn test_price_caps_the_tip_and_adds_the_headroom() {
        // 100 gwei base + 20 bps (= 0.2 gwei) + 2 gwei capped tip.
        let (max_fee, tip) = price_eip1559(100 * GWEI, 10 * GWEI, &config());
        assert_eq!(tip, 2 * GWEI);
        assert_eq!(max_fee, 100 * GWEI + 200_000_000 + 2 * GWEI);
    }

    #[test]
    fn test_price_passes_a_tip_below_the_cap_through() {
        let (max_fee, tip) = price_eip1559(100 * GWEI, GWEI, &config());
        assert_eq!(tip, GWEI);
        assert_eq!(max_fee, 101 * GWEI + 200_000_000);
    }

    #[test]
    fn test_price_without_a_base_fee_is_just_the_tip() {
        let (max_fee, tip) = price_eip1559(0, GWEI, &config());
        assert_eq!((max_fee, tip), (GWEI, GWEI));
    }

    #[test]
    fn test_price_saturates_instead_of_wrapping() {
        let (max_fee, tip) = price_eip1559(u128::MAX, u128::MAX, &config());
        assert_eq!(max_fee, u128::MAX);
        assert_eq!(tip, 2 * GWEI);
        // An astronomic cap saturates the cap itself too.
        let cfg = GasConfig { max_priority_fee_gwei: u128::MAX, ..config() };
        let (_, tip) = price_eip1559(1, u128::MAX, &cfg);
        assert_eq!(tip, u128::MAX);
    }

    #[test]
    fn test_bump_compounds_per_attempt() {
        // 1000 -> 1150 -> 1322 (integer truncation at each step).
        let (max_fee, tip) = bump(1_000_000, 1_000, 2, &config());
        assert_eq!(max_fee, 1_322_500);
        assert_eq!(tip, 1_322);
    }

    #[test]
    fn test_bump_attempt_zero_is_a_no_op() {
        let (max_fee, tip) = bump(50_000, 1_000, 0, &config());
        assert_eq!((max_fee, tip), (50_000, 1_000));
    }

    #[test]
    fn test_bump_recaps_the_tip() {
        // The previous tip is above the 2 gwei cap; it is pulled back.
        let (max_fee, tip) = bump(100 * GWEI, 5 * GWEI, 1, &config());
        assert_eq!(tip, 2 * GWEI);
        assert_eq!(max_fee, 115 * GWEI);
    }

    #[test]
    fn test_bump_never_returns_below_the_previous_fee() {
        // A zero bump keeps both values...
        let cfg = GasConfig { bump_pct: 0, ..config() };
        assert_eq!(bump(50_000, 1_000, 7, &cfg), (50_000, 1_000));
        // ...and an actual bump keeps the fee cap at or above the previous
        // one.
        let (max_fee, tip) = bump(50_000, 1_000, 1, &config());
        assert_eq!((max_fee, tip), (57_500, 1_150));
    }

    #[test]
    fn test_bump_floors_the_fee_cap_at_the_tip() {
        // A previous fee cap below the re-capped tip would produce an
        // invalid EIP-1559 transaction; the floor lifts it to the tip.
        let (max_fee, tip) = bump(1_000, 5 * GWEI, 1, &config());
        assert_eq!((max_fee, tip), (2 * GWEI, 2 * GWEI));
    }

    #[test]
    fn test_bump_saturates_at_u128_max() {
        let (max_fee, tip) = bump(u128::MAX, u128::MAX, 5, &config());
        assert_eq!(max_fee, u128::MAX);
        assert_eq!(tip, 2 * GWEI);
    }

    #[test]
    fn test_bump_saturates_a_runaway_attempt_counter() {
        // One gwei, the smallest realistic fee, with the smallest possible
        // bump: the capped loop still compounds it to saturation.
        let cfg = GasConfig { bump_pct: 1, max_priority_fee_gwei: u128::MAX, ..config() };
        let (max_fee, tip) = bump(GWEI, GWEI, u32::MAX, &cfg);
        assert_eq!(max_fee, u128::MAX);
        assert_eq!(tip, u128::MAX);
    }

    #[test]
    fn test_gas_limit_applies_buffer_and_cap() {
        assert_eq!(gas_limit(1_000_000, &config()), 1_100_000);
        // 29M + 10% = 31.9M, over the 30M cap.
        assert_eq!(gas_limit(29_000_000, &config()), 30_000_000);
        assert_eq!(gas_limit(0, &config()), 0);
    }

    #[test]
    fn test_gas_limit_saturates() {
        let cfg = GasConfig { gas_buffer_pct: u16::MAX, gas_limit_cap: u64::MAX, ..config() };
        assert_eq!(gas_limit(u64::MAX, &cfg), u64::MAX);
        assert_eq!(gas_limit(u64::MAX, &config()), 30_000_000);
    }
}
