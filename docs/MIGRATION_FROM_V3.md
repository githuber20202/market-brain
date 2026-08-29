# Migration from V3

V3 is retired as an architectural direction. Its market feature and scoring concepts are preserved, but the account gate and private-account control plane are removed.

Replacements:

- account snapshot -> Software Risk Wallet
- external position read -> User-confirmed Position Twin
- candidate watchlist -> Internal Candidate Projection
- fixed T checkpoints -> Market phases plus event triggers
- broker verification -> Authoritative market-data verification
- account-aware BUY_NOW -> Wallet-aware BUY_NOW
- account-aware SELL_NOW -> Position-Twin-aware SELL_NOW

Migration is fail closed. Legacy positions are not inferred. The user must import or confirm them explicitly before the system manages exits.

