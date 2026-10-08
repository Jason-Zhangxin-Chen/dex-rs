# The on-chain settlement protocol (assumed design)

> **This protocol is not frozen.** The contracts below are the assumed
> interfaces the off-chain services ([SVD_Pretrade], [SVD_Sync],
> [SVD_Settlement]) are designed against. When the real contracts land, only
> the bindings in the services change — the service specs (margin pre-checks,
> event syncing, failure classification) stay. Every assumption that the
> service specs depend on is stated here, so changing a contract interface
> means updating this document first.

The protocol is EVM compatible; the workspace pins `alloy` for chain interop
(ABI encoding, transports, signature primitives).

## The settlement model

The protocol settles trades **in cash against a single quote asset** (e.g.
USDC): it does not move the base asset per trade. Positions are ledger
entries per `(account, symbol)` inside the protocol; a buy increases a long
position, a sell decreases it (shorts are allowed). Margin is per account and
**global across symbols** — the account's equity backs every position.

- **Equity** — the account's collateral in the quote asset.
- **Used margin** — the margin locked by the account's open positions,
  computed per symbol with the symbol's margin ratio.
- **Available margin** — `equity − used margin`; the settlement of a trade
  reverts when the trade would push the account's available margin below
  zero.

Prices and quantities on-chain are the same raw integers as off-chain:
`uint64` price ticks and `uint64` lot units (see the `Price` / `Quantity`
value types). The symbol configuration converts them into quote-asset
amounts: `notional = price × quantity × quotePerTickLot`, fees as bps, margin
requirement as bps of notional.

## The order hash

Both parties of a trade sign their orders; the signature is verified
off-chain by [SVD_Pretrade] and on-chain by the settlement protocol, so both
sides must hash the identical structure. The assumed scheme is EIP-712.

```solidity
struct Order {
    bytes32 symbol;      // the 32-byte symbol, the same encoding as off-chain
    address user;
    uint64 nonce;        // per-user order nonce
    uint64 price;        // limit price, raw ticks
    uint64 quantity;     // visible quantity, lot units
    uint64 totalQuantity;// visible + hidden (iceberg / reserve), lot units
    uint8 side;          // 0 = buy, 1 = sell
    uint64 timeInForce;  // packed tag: low byte = the off-chain TimeInForce tag
                         // (0 gtc, 1 ioc, 2 fok, 3 gtd, 4 day), the next byte =
                         // the GTD lifetime in hours — the EIP-712 hash layout
                         // (see the order hash section), NOT a plain uint8
    uint64 timestampMs;  // creation time
}
```

The EIP-712 domain is `{ name: "dex-rs", version: "1", chainId,
verifyingContract: settlement }`. The off-chain [`Order`] keeps more data
(order kind parameters, the id); the fields above are the subset the two
signers commit to, so they must be computed identically everywhere.
[SVD_Pretrade] verifies the signature against this hash at admission; the
contract verifies both signatures again at settlement (defense in depth —
the contract is the final arbiter).

## The contracts

Two contracts, assumed split so the margin state is owned by one authority:

### MarginAccount

Owns the per-account margin state and the deposit / withdraw flows.

```solidity
interface IMarginAccount {
    /// Deposits quote-asset collateral for `msg.sender`.
    function deposit(address account, uint256 amount) external;
    /// Withdraws available margin. Withdrawal delays are out of scope here.
    function withdraw(uint256 amount) external;

    /// The margin state of an account.
    function equity(address account) external view returns (uint256);
    function usedMargin(address account) external view returns (uint256);
    function availableMargin(address account) external view returns (uint256);

    /// State is mutated only by the Settlement contract; every mutation
    /// emits MarginAccountUpdated.
    function updateForSettlement(
        address account,
        int256 equityDelta,
        uint256 usedMarginAfter
    ) external;

    event MarginDeposited(address indexed account, uint256 amount, uint256 equityAfter);
    event MarginWithdrawn(address indexed account, uint256 amount, uint256 equityAfter);
    /// Emitted after every state change — the single event [SVD_Sync]
    /// subscribes to as the authoritative margin state.
    event MarginAccountUpdated(
        address indexed account,
        uint256 equity,
        uint256 usedMargin,
        uint256 availableMargin
    );
}
```

### Settlement

Owns the per-symbol configuration and the batch settlement entry point.
Submissions are made by the settlement operator (the [SVD_Settlement]
service's hot wallet); the contract verifies the parties' signatures and the
margin for every trade in the batch.

```solidity
interface ISettlement {
    /// A matched cross: two signed orders and the execution of that cross.
    struct MatchedTrade {
        Order taker;
        bytes takerSignature;  // 65 bytes (r, s, v)
        Order maker;
        bytes makerSignature;
        uint64 price;          // the executed price of this cross, raw ticks
        uint64 tradedQuantity; // this cross's quantity, lot units
        uint64 takerRemaining; // the taker's remaining total after this cross
    }

    /// Settles a batch of matched trades. All-or-nothing: one failing trade
    /// reverts the whole batch with the index and the reason.
    function settleBatch(MatchedTrade[] calldata trades) external;

    /// Per-symbol configuration, set by the operator. The off-chain configs
    /// of [SVD_Pretrade] and [SVD_OMS_Master] must be kept in lockstep with
    /// these (an ops invariant, see the pre-trade spec).
    function setSymbolConfig(
        bytes32 symbol,
        uint64 tickSize,
        uint64 lotSize,
        uint256 quotePerTickLot,
        uint32 feeBps,
        uint32 marginRatioBps
    ) external;

    function pause() external;
    function unpause() external;

    event TradeSettled(
        bytes32 indexed symbol,
        bytes32 indexed takerOrderHash,
        bytes32 indexed makerOrderHash,
        address taker,
        address maker,
        uint64 price,
        uint64 quantity,
        uint256 takerEquityAfter,
        uint256 makerEquityAfter
    );
    event SymbolConfigured(bytes32 indexed symbol, uint32 feeBps, uint32 marginRatioBps);
    event SettlementPaused(bool paused);
}

/// The failure taxonomy of settleBatch. The revert data carries the code, the
/// index of the failing trade in the batch, and the at-fault side (1 = taker,
/// 2 = maker, 0 = neither); [SVD_Settlement] decodes it and dispatches the
/// recovery actions (see its spec).
error SettlementError(uint8 code, uint256 index, uint8 side);
```

### The failure taxonomy

| code | error | meaning | settlement action |
| --- | --- | --- | --- |
| 1 | `InvalidSignature` | a party's signature does not recover to the order's user | remove the at-fault order (cancel it on the book); the innocent side's crossed quantity is restored through the pre-trade pipeline — never retry |
| 2 | `InsufficientMargin` | the trade would push an account's available margin below zero | remove the at-fault account's orders and block it until [SVD_Sync] observes recovered equity; the innocent side's crossed quantity is restored through the pre-trade pipeline |
| 5 | `SymbolPaused` | the symbol is not accepting settlement | transient: retry with backoff until the pause lifts — no outcome is published while the batch is pending |
| 6 | `SettlementPaused` | the protocol is paused | transient: retry with backoff until the pause lifts — no outcome is published while the batch is pending |

The taxonomy is deliberately short: the contract is **stateless** per order — it keeps no
filled-quantity ledger, so it cannot detect a duplicate settlement, an expired order, or an
invalid price / quantity. Those validations belong to the off-chain book
([SVD_OMS_Master] is the single source of truth), and the codes for them do not exist.

## settleBatch semantics

For each trade in the batch, in order:

1. **Symbol check** — the symbol is configured and not paused; both orders
   carry the same symbol.
2. **Signature check** — `ecrecover` both signatures against the EIP-712
   order hash; the recovered addresses must equal the orders' users.
3. **Margin check and apply** — compute the notional from the executed
   price, apply the fees, update both positions, and compute the used
   margin after; revert with `InsufficientMargin` if either account's
   available margin would fall below zero. Mutating the margin state
   emits `MarginAccountUpdated` for both accounts, and a settled trade
   emits `TradeSettled`.

There is no order-lifecycle step: the contract keeps no per-order state (no
filled-quantity ledger), so it settles whatever the operator submits whose
signatures recover and whose margin holds — the off-chain book
([SVD_OMS_Master]) is the single source of truth for fill accounting, order
expiry and price / quantity validation.

Any failing trade reverts the whole batch (all-or-nothing) with
`SettlementError(code, index)`. The [SVD_Settlement] service exploits the
all-or-nothing semantics to isolate a failing trade: it binary-splits the
batch, resubmits the halves that settle, and applies the per-code action to
the failing half.

## What the protocol does not cover

- **Order admission** — orders never enter the chain; the chain sees a trade
  only after the off-chain engine matched it. The contract has no order book.
- **Order lifecycle and trade validation** — the contract is stateless per
  order: it tracks no filled quantities and checks no timestamps, prices or
  quantities, so it cannot detect a duplicate settlement, an expired order or
  a bad trade. Those validations are the off-chain book's job; the
  [SVD_Settlement] journal's at-most-once publication is the
  duplicate-submission guard on the operator side.
- **Off-chain margin checks** — [SVD_Pretrade] checks the orders against the
  latest synced margin state without off-chain reservations (a reservation
  mechanism may be added later): the sync window tolerates over-subscription,
  the contract enforces the final margin at settlement time, and the trades
  that cannot settle are removed from the book with the account blocked until
  fresh margin state arrives.
- **Gas batching policy, nonce management, reorg handling** — those belong to
  [SVD_Settlement].
- **Deposit / withdraw flows** (KYC, withdrawal delays, oracles) — only the
  state changes they produce matter to the services here.
