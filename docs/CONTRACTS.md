# Market kernel contracts

Status: **draft contract for Task 33B review**. This document defines interchange
formats only. It does not activate a shared kernel, change scoring, alter weights or
thresholds, publish a live artifact, or authorize broker/account access.

The normative words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are used in their
usual RFC sense. A consumer MUST fail closed on an unknown schema version, an invalid
required field, a digest mismatch, or a non-canonical test-vector result.

## Boundary and invariants

- `market-observation.v1` contains market evidence only and is the scoring/gating
  input.
- `market-decision-record.v1` contains the market-only result. Its strongest gate is
  `MARKET_READY_ELIGIBLE`; it is never an order instruction or final live `READY`.
- The private `account_risk_gate` is outside both public contracts. It MAY downgrade
  a market-eligible result, but it MUST NOT upgrade `WATCH` or `REJECT`.
- The live artifact is private and MUST NOT be published. Any future public record is
  a new allowlist projection described below, never the live artifact itself.
- `first_seen_at`, not article publication time or bar time alone, controls causal
  credit. Evidence first observed after `decision_at` MUST NOT influence that
  decision.
- This contract does not define or revise scores, weights, cutoffs, freshness limits,
  setup geometry, or risk limits. Those remain in the versioned policy bundle.

## Shared types

### Timestamps

Every timestamp MUST be an RFC 3339 string with an explicit offset. Canonical form is
UTC with six fractional digits and `Z`, for example
`2026-09-03T13:18:00.000000Z`. A timestamp without an offset is invalid.

The relevant clocks are distinct:

| Field | Meaning |
|---|---|
| `scheduled_for` | Policy slot that the observation or decision is intended to measure. |
| `market_event_at` | When the quote, bar, or news event occurred at its source. |
| `first_seen_at` | Earliest durable time this system can prove it possessed that evidence. |
| `as_of` | Observation cutoff; no later evidence may be included. |
| `decision_at` | Time supplied by the orchestrator when it invokes the deterministic decision boundary. |

For every credited evidence item:

`market_event_at <= first_seen_at <= as_of <= decision_at`

If that ordering cannot be proven, the affected component MUST be `CONFLICT` and the
market gate MUST fail closed. A historical source timestamp MUST NOT be substituted
for `first_seen_at`.

For delayed bars fetched by a batch, `first_seen_at` is that batch's recorded
`fetched_at`—the first provable possession time—not the bar timestamp. For news found
through Yahoo Search, it is the first recorded fetch time, never
`providerPublishTime`; that provider value belongs in `market_event_at`.

### Evidence/component state

Every evidence group and every score component has exactly one state:

| State | Meaning |
|---|---|
| `VERIFIED` | Present, schema-valid, provenance-valid, causal, and within the existing policy's age rules. |
| `MISSING` | A required value or evidence group was not available. |
| `DELAYED` | Present and causal, but older than the existing policy permits for this stage. |
| `CONFLICT` | Sources, timestamps, provenance, or internally related values disagree. |

States describe evidence quality; they do not award points. The policy decides the
existing fail-closed consequence. Where multiple defects apply, canonical severity is
`CONFLICT`, then `MISSING`, then `DELAYED`, then `VERIFIED`.

The declaration of `required_evidence_groups` and `required_components` for every
stage MUST live in the Task 34 policy bundle. If a stage has no such declaration,
Shadow MUST keep `coverage.complete=false`. The current keyless path MUST represent
its lawful evidence rather than invent unavailable fields: relative strength is
measured against SPY, and spread uses Cboe when available. A field that its source
does not provide is `MISSING` or `DELAYED` according to the existing policy—not
`CONFLICT`, which is reserved for disagreement or an invalid invariant.

### Source provenance

Each quote, bar set, and news item MUST carry:

- `source`: stable provider name such as `IBKR` or `YAHOO`;
- `source_id`: provider record/request identifier, or `null` if the provider supplies
  none;
- `market_event_at`, `first_seen_at`, and `fetched_at`;
- `delay_minutes`: non-negative age from market event to proven first observation,
  rounded under the canonical-number rule, or `null` when it cannot be established;
- `state` and canonical `reason_codes`.

`source` is provenance, not authority. Authority remains stage- and policy-specific.
Every reason code MUST match `^[A-Z][A-Z0-9_]*$`.

## `market-observation.v1`

### Top-level object

The object is closed: keys not listed here are invalid in v1.

| Field | Type | Required | Description |
|---|---:|---:|---|
| `schema_version` | string | yes | Literal `market-observation.v1`. |
| `session_id` | string | yes | New York market date, `YYYY-MM-DD`. |
| `batch_id` | string | yes | Stable identifier for one scheduled evaluation batch. |
| `stage` | string | yes | Canonical policy stage name. |
| `symbol` | string | yes | Uppercase listed-security symbol. |
| `scheduled_for` | timestamp | yes | Intended policy slot. |
| `as_of` | timestamp | yes | Evidence cutoff used to build this observation. |
| `quote` | object | yes | Quote evidence; declared nullable values remain present. |
| `bars` | object | yes | Intraday bar, opening-range, and retest evidence. |
| `catalyst` | object | yes | Catalyst/continuation evidence. |
| `references` | object | yes | Benchmark, sector, and prior-session reference values. |
| `market_plan` | object or null | yes | Existing market-only plan geometry, if one exists. |
| `component_states` | object | yes | Readiness state for each of the six score components. |
| `coverage` | object | yes | Evidence coverage before evaluation. |

### Quote

`quote` has these exact keys:

```json
{
  "ask": 101.02,
  "bid": 101,
  "delay_minutes": 0.0167,
  "fetched_at": "2026-09-03T13:18:01.000000Z",
  "first_seen_at": "2026-09-03T13:18:01.000000Z",
  "last": 101.01,
  "market_event_at": "2026-09-03T13:18:00.000000Z",
  "reason_codes": [],
  "source": "IBKR",
  "source_id": null,
  "state": "VERIFIED",
  "volume": 250000,
  "vwap": 100.42
}
```

`last`, `bid`, `ask`, `volume`, and `vwap` are required nullable JSON numbers.
Prices MUST be positive when present; volume MUST be non-negative. Crossed BBO,
negative values, or mutually inconsistent quote fields require `CONFLICT`.

### Bars, opening range, and retest

`bars` has exactly the keys `state`, `reason_codes`, `source`, `source_id`,
`market_event_at`, `first_seen_at`, `fetched_at`, `delay_minutes`, `items`,
`opening_range`, and `retest`. Its `market_event_at` is the latest market-event time
included in `items`; its other provenance fields apply to the complete set. It also
contains:

- `items`: an array ordered by `market_event_at`, with exact fields `open`, `high`,
  `low`, `close`, `volume`, `vwap`, `market_event_at`, and `first_seen_at`;
- `opening_range`: object with required nullable fields `high`, `low`, `start_at`,
  `end_at`, `bars_expected`, and `bars_observed`;
- `retest`: object with required nullable fields `state`, `breakout_at`, `retest_at`,
  `low`, and `close`.

Bar order is semantic and MUST NOT be re-sorted after canonical construction.
Duplicate source bars for the same symbol/minute MUST be resolved by the existing
source-authority policy before kernel invocation, or the group is `CONFLICT`.

### Catalyst evidence

`catalyst` has exact keys `state`, `reason_codes`, `strength`, `continuation`, and
`items`. `strength` is a required nullable number; `continuation` is a required
nullable boolean. Each item has:

```json
{
  "classification": "EARNINGS",
  "delay_minutes": 3.25,
  "fetched_at": "2026-09-03T12:03:15.000000Z",
  "first_seen_at": "2026-09-03T12:03:15.000000Z",
  "headline": "Example public headline",
  "market_event_at": "2026-09-03T12:00:00.000000Z",
  "reason_codes": [],
  "source": "YAHOO",
  "source_id": "public-item-id",
  "state": "VERIFIED",
  "url": "https://example.invalid/item"
}
```

Items are ordered by `first_seen_at`, then `source`, then `source_id`, treating a
`null` source ID as an empty string for this comparison. A headline found after the
decision may appear only in a later observation; it cannot be backfilled into the
earlier record.

### References

`references` has exactly the keys `prior_close`, `average_volume`,
`benchmark_return_pct`, and `sector_return_pct`. Each maps to an object with exact
keys `value`, `state`, `reason_codes`, `source`, `source_id`, `market_event_at`,
`first_seen_at`, `fetched_at`, and `delay_minutes`. `value` is required nullable. A
producer MUST NOT claim one source for mixed data.

### Market-only plan

`market_plan` is `null` when no plan existed as of the observation. Otherwise it has
the exact keys:

`plan_id`, `created_at`, `expires_at`, `entry_trigger`, `entry_zone_high`,
`invalidation`, `target_1`, `target_2`, `max_spread_pct`, and `max_slippage_pct`.

These are policy-derived market levels, not actual orders or fills. Every numeric
field is required nullable. A plan created after `as_of`, or a plan based on evidence
first seen later, is invalid for the observation.

### Component readiness

`component_states` contains exactly these six keys:

- `catalyst_or_continuation`
- `price_momentum`
- `volume_liquidity`
- `relative_strength_sector`
- `entry_invalidation_structure`
- `risk_reward`

Each value is an object with exact keys `state` and `reason_codes`. No numeric score
belongs in `component_states`.

### Observation coverage

`coverage` has exact keys:

- `required_evidence_groups`: ordered list of group names required at this stage;
- `verified_evidence_groups`: ordered list of groups in `VERIFIED` state;
- `missing_evidence_groups`, `delayed_evidence_groups`, and
  `conflict_evidence_groups`: canonical ordered lists;
- `universe_expected`, `universe_observed`: required nullable non-negative integers;
- `causality_ok`: boolean.

Coverage is descriptive and MUST NOT hide missing data by reporting zero expected.

## `market-decision-record.v1`

### Top-level object

The object is closed and has these exact keys:

| Field | Type | Description |
|---|---:|---|
| `schema_version` | string | Literal `market-decision-record.v1`. |
| `session_id` | string | Copied from the observation. |
| `batch_id` | string | Copied from the observation. |
| `stage` | string | Copied from the observation. |
| `symbol` | string | Copied from the observation. |
| `scheduled_for` | timestamp | Copied from the observation. |
| `decision_at` | timestamp | Supplied explicitly by the orchestrator; the kernel MUST NOT read a clock. |
| `engine` | object | Exact engine identity described below. |
| `feature_vector` | object | Normalized market features and a state per feature. |
| `score_components` | object | Six existing components, each with value and state. |
| `total` | number or null | Existing total; `null` if policy cannot lawfully calculate it. |
| `market_gate` | object | Market-only eligibility and reasons. |
| `input_digest` | string | SHA-256 of canonical `market-observation.v1` bytes. |
| `coverage` | object | Decision-level component and evidence coverage. |

### Engine identity

`engine` has exact keys:

| Field | Rule |
|---|---|
| `policy_git_commit_sha` | 40-character lowercase Git commit SHA containing the exact loaded policy/kernel sources. |
| `policy_bundle_sha256` | Lowercase SHA-256 of the canonical market-policy bundle manifest defined below. |
| `kernel_sha256` | Lowercase SHA-256 of the raw repository-blob bytes of `src/market_brain/policy/kernel.py`; no Git blob header and no line-ending conversion. |
| `policy_version` | Human-readable immutable release label. |

The legacy `policy_release_sha` is not overloaded with these meanings and MUST NOT be
substituted for any of the three hashes.

The market-policy bundle manifest has this closed shape:

```json
{
  "files": [
    {
      "path": "config/example-market-policy.json",
      "sha256": "<SHA-256 of raw repository-blob bytes>"
    }
  ],
  "policy_version": "2026-09-02.1",
  "schema_version": "market-policy-bundle.v1"
}
```

`files` includes every market rule/parameter file actually loaded, excludes the
kernel (which has its own hash), excludes `POLICY_RELEASE.json` to avoid
self-reference, and excludes all account/private policy. Entries are sorted by
canonical `path`. `policy_bundle_sha256` is computed over this manifest's canonical
bytes including LF.

### Feature vector

`feature_vector` contains the current normalized market features required by the
policy. For v1 the exact keys are:

`price_return_pct`, `gap_pct`, `relative_volume`, `spread_pct`,
`distance_from_vwap_pct`, `relative_strength_pct`, `catalyst_strength`, and
`liquidity_ok`.

Each key maps to an object with exact keys `value`, `state`, `source_paths`, and
`reason_codes`. `value` is a JSON number, boolean, or `null`; `source_paths` is a
canonical ordered array of JSON-pointer paths into the observation. This makes every
derived feature traceable without embedding the original private artifact.

### Score components and total

`score_components` contains exactly the same six names as `component_states`. Each
component has exact keys:

```json
{
  "reason_codes": [],
  "state": "VERIFIED",
  "value": 20
}
```

`value` is required but nullable. A non-`VERIFIED` component MUST retain its true
state and policy reason; it MUST NOT receive a fabricated default. `total` MUST be
the result of the existing policy applied to these components. This contract adds no
fallback points and changes no weight.

### Market gate

`market_gate` has exact keys `status` and `reason_codes`. `status` is one of:

- `MARKET_READY_ELIGIBLE`: market evidence and market policy permit the private live
  layer to evaluate account risk;
- `WATCH`: keep observing; market requirements are not yet all satisfied;
- `REJECT`: market policy invalidates the candidate for this decision.

Reason codes are machine identifiers, not prose. `MARKET_READY_ELIGIBLE` is not
`READY`, does not size a position, and does not authorize execution. The private live
decision is conceptually:

`final_live_gate = market_gate AND account_risk_gate`

Only the private live layer may evaluate the second operand.

### Decision coverage

`coverage` has exact keys:

- `required_components`: literal ordered list of the six component names;
- `verified_components`, `missing_components`, `delayed_components`, and
  `conflict_components`: canonical ordered lists;
- `evidence`: the observation coverage object copied without alteration;
- `causality_ok`: boolean;
- `complete`: boolean, true only when every required component is `VERIFIED`, all
  required evidence exists, and causality is valid.

`complete=false` MUST prevent `MARKET_READY_ELIGIBLE` unless the already-versioned
policy explicitly declares that component non-required for that stage.

## Deterministic evaluation context

The scoring/gating kernel consumes `market-observation.v1`. It MUST perform no I/O and
MUST NOT read a clock, environment variable, database, network, account, or random
source. The surrounding record assembler supplies this immutable context:

```json
{
  "decision_at": "2026-09-03T13:18:02.000000Z",
  "engine": {
    "kernel_sha256": "<64 lowercase hex>",
    "policy_bundle_sha256": "<64 lowercase hex>",
    "policy_git_commit_sha": "<40 lowercase hex>",
    "policy_version": "2026-09-02.1"
  }
}
```

The complete deterministic boundary is therefore `(observation, context) ->
decision record`. Live and Shadow MAY use different data adapters, but identical
canonical inputs and context MUST produce byte-identical canonical decision records.

## Canonical JSON

Canonical bytes are produced in this order:

1. Validate the closed schema and normalize timestamps to canonical UTC form.
2. Convert every finite numeric input from its exact decimal text representation.
   Round to at most four fractional digits using decimal round-half-even. Render the
   shortest plain base-10 form after rounding, remove trailing fractional zeros, and
   render negative zero as `0`. Exponents, `NaN`, and infinities are forbidden.
3. Remove duplicate reason codes and sort them by uppercase ASCII byte order.
   Component coverage lists follow the six-component declaration order. Evidence
   coverage lists follow `required_evidence_groups`; an undeclared group is invalid.
   Other arrays preserve their schema-defined semantic order.
4. Sort object keys recursively by Unicode code point. Use double-quoted JSON strings,
   standard JSON escaping, no ASCII-only escaping, and no insignificant whitespace.
5. Encode as UTF-8 without BOM and append exactly one LF (`0x0a`).

All declared required fields MUST be present. `null` means “declared and currently
unknown.” A missing required key means “invalid record,” not `null`. An empty array
means the producer proved there are zero items. Producers MUST NOT use an omitted key,
empty string, zero, or empty object as a substitute for `null`.

`input_digest` is lowercase SHA-256 over the complete canonical observation bytes,
including the final LF. Digest comparison occurs before evaluation.

## Durable artifacts

A write is `DURABLE` only when all five conditions hold:

1. the payload validates against its named schema;
2. it is written to a deterministic path derived from schema version, `session_id`,
   `batch_id`, `stage`, `symbol`, and payload digest;
3. the canonical-byte SHA-256 is stored with the artifact or in its manifest;
4. storage survives process/job restart and is included in the declared retention
   policy;
5. an immediate read-after-write returns byte-identical content and the same digest.

The canonical path template is:

`<store>/<schema_version>/<session_id>/<batch_id>/<stage>/<symbol>/<sha256>.json`

If any condition is unproven, the writer result is `LOCAL_WRITTEN`. `LOCAL_WRITTEN`
is not a checkpoint, MUST NOT count toward coverage, and MUST block learning that
depends on it. A temporary file, chat attachment, job workspace, or successful local
`write()` alone is never durable evidence.

The storage receipt is outside the hashed market payload and has exact keys
`artifact_status`, `path`, `sha256`, `persisted_at`, and `readback_sha256`.

## Public allowlist projection

Projection constructs a new object by copying approved paths. It MUST NOT serialize
the live object and delete “known private” fields. Unknown fields are never copied,
and an output key outside the public schema fails with `PUBLIC_PROJECTION_BLOCKED`.

No projection is activated by this document. Task 35 may project only these paths:

| Source | Publicly eligible paths |
|---|---|
| Observation identity | `schema_version`, `session_id`, `batch_id`, `stage`, `symbol`, `scheduled_for`, `as_of` |
| Quote | `quote.{last,bid,ask,volume,vwap,source,market_event_at,first_seen_at,fetched_at,delay_minutes,state,reason_codes}` |
| Bars group | `bars.{source,market_event_at,first_seen_at,fetched_at,delay_minutes,state,reason_codes,opening_range,retest}` |
| Bar items | `bars.items[].{open,high,low,close,volume,vwap,market_event_at,first_seen_at}` |
| Catalyst group | `catalyst.{state,reason_codes,strength,continuation}` |
| Catalyst items | `catalyst.items[].{classification,delay_minutes,fetched_at,first_seen_at,headline,market_event_at,reason_codes,source,state}` |
| References | `references.{prior_close,average_volume,benchmark_return_pct,sector_return_pct}.{value,state,reason_codes,source,market_event_at,first_seen_at,fetched_at,delay_minutes}` |
| Plan | `market_plan.{plan_id,created_at,expires_at,entry_trigger,entry_zone_high,invalidation,target_1,target_2,max_spread_pct,max_slippage_pct}` |
| Readiness | `component_states`, `coverage` |
| Decision identity | `schema_version`, `session_id`, `batch_id`, `stage`, `symbol`, `scheduled_for`, `decision_at` |
| Engine/result | `engine`, `feature_vector`, `score_components`, `total`, `market_gate`, `input_digest`, `coverage` |

The following are private and MUST NEVER leave the private live layer, even if a
future source object adds similarly named fields:

- account number, account alias, broker login, user identity, tenant/workspace ID;
- access tokens, cookies, API keys, connector payloads, request headers, or secrets;
- cash, equity, buying power, margin, balances, account limits, or risk budget;
- holdings, position quantity, target quantity, reserved quantity, or allocation;
- order IDs, perm IDs, execution IDs, fill quantity, actual fill price/time, fees,
  commissions, realized/unrealized P&L, or tax data;
- private stops, account-derived size, account exposure, portfolio correlation, or
  account-specific constraints;
- `account_risk_gate`, its status, inputs, reasons, overrides, or audit details;
- private notes, screenshots, file paths, machine/user names, IP addresses, and raw
  broker responses.

The projection implementation MUST use an explicit positive allowlist and a closed
public-output schema. A blocklist is prohibited.

## Kernel test vectors

Task 34 will place vectors under:

`tests/vectors/kernel/<case-name>.json`

Task 33B creates no executable vectors. Each future file has this exact envelope:

```json
{
  "context": {
    "decision_at": "2026-09-03T13:18:02.000000Z",
    "engine": {
      "kernel_sha256": "<64 lowercase hex>",
      "policy_bundle_sha256": "<64 lowercase hex>",
      "policy_git_commit_sha": "<40 lowercase hex>",
      "policy_version": "2026-09-02.1"
    }
  },
  "expected_output": {
    "schema_version": "market-decision-record.v1"
  },
  "expected_output_sha256": "<64 lowercase hex>",
  "input": {
    "schema_version": "market-observation.v1"
  },
  "name": "verified-market-example",
  "vector_schema_version": "kernel-test-vector.v1"
}
```

The shown nested payloads are abbreviated only in this documentation; real vectors
MUST contain complete schema-valid objects. `expected_output_sha256` is the SHA-256
of canonical `expected_output` bytes including the final LF.

Both GitHub Shadow and the private live consumer MUST:

1. validate the vector envelope and complete input;
2. verify the input digest;
3. evaluate without I/O;
4. compare the full output structure;
5. compare byte-identical canonical output and the expected SHA-256.

Required vector classes are: fully verified eligibility, each missing component,
delayed quote, conflicting BBO, evidence first seen after decision, absent plan,
`WATCH`, `REJECT`, numeric rounding boundaries, canonical reason ordering, and invalid
schema/digest rejection. Passing vectors proves rule parity for identical inputs; it
does not prove Yahoo and IBKR observations are equivalent.

## Versioning and failure behavior

- v1 is immutable after adoption. Breaking changes require `v2`.
- Additive unknown keys are breaking for these closed schemas and fail validation.
- Consumers MUST retain the original schema version and hashes in reconciliation.
- A validation, causality, canonicalization, or digest failure produces no
  `MARKET_READY_ELIGIBLE`; it records a stable reason code and fails closed.
- No consumer may infer missing values, assign placeholder score points, or rewrite
  historical `first_seen_at` during replay.
