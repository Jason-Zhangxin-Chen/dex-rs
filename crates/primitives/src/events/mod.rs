pub mod book_state_ev;
pub mod control_ev;
pub mod order_ev;
pub mod order_state_ev;
pub mod trade_ev;

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::address::Address;
    use crate::base::{Hash32, Nonce, Side, Symbol};
    use crate::order::{Order, OrderCold, OrderColdCommon, OrderHot, OrderKind};
    use crate::signature::Signature;
    use crate::time_in_force::TimeInForce;
    use crate::value::{Price, Quantity, TimestampMs};
    use serde::Serialize;

    /// Builds a sample [`Order`] for tests.
    ///
    /// `Order` has no public constructor yet and the fields of [`OrderColdCommon`] are
    /// private, so the cold common data is round-tripped through the wire format via a
    /// mirror struct that must stay in sync with its serialized shape.
    pub(crate) fn sample_order(seed: u8) -> Order {
        let common_mirror = OrderColdCommonMirror {
            id: Hash32([seed; 32]),
            symbol: Symbol([seed.wrapping_add(1); 32]),
            signature: Signature([seed.wrapping_add(2); 65]),
            timestamp: TimestampMs(u64::from(seed) * 1_000),
        };
        let bytes = rmp_serde::to_vec(&common_mirror).unwrap();
        let common: OrderColdCommon = rmp_serde::from_slice(&bytes).unwrap();
        Order {
            hot: OrderHot {
                user: Address([seed.wrapping_add(3); 20]),
                nonce: Nonce(u64::from(seed)),
                price: Price(u64::from(seed) * 10),
                quantity: Quantity(u64::from(seed) * 100),
                time_in_force: TimeInForce::Gtc,
                side: Side::Buy,
            },
            cold: OrderCold { common, kind: OrderKind::Standard },
        }
    }

    /// Mirror of [`OrderColdCommon`]'s wire shape, see [`sample_order`].
    #[derive(Serialize)]
    struct OrderColdCommonMirror {
        id: Hash32,
        symbol: Symbol,
        signature: Signature,
        timestamp: TimestampMs,
    }
}
