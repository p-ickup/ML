# Schema & data

What the pipeline reads, writes, and holds in memory. Table shapes reflect **fields used by this codebase**; the live Supabase schema may have additional columns managed by the main Pickup app.

## In-memory: `RiderLite`

Built in `Algorithm/rider_data.py` from one `Flights` row + one `Users` row.

| Field | Source | Notes |
|-------|--------|-------|
| `user_id` | Flights | |
| `flight_id` | Flights | Primary flight identifier |
| `flight_no`, `airline_iata` | Flights | Same-flight matching bonus |
| `earliest_time`, `latest_time` | Flights | Pickup window (time-of-day strings) |
| `airport` | Flights | Normalized IATA (e.g. `LAX`, `ONT`) |
| `to_airport` | Flights | `true` = school → airport (`TO`) |
| `date` | Flights | Ride date (`YYYY-MM-DD`) |
| `terminal` | Flights | Normalized terminal code |
| `school`, `name` | Users | Skipped if `school` is missing |
| `bags_no`, `bags_no_large`, `bag_no_personal` | Flights | Bag counts |
| `matched` | Flights | Carried through; pipeline input is unmatched only |
| `subsidized` | Set later | Per-rider flag during subsidy pass |

## In-memory: `Match`

Built in `Algorithm/ruleMatching.py`; one object = one ride group.

| Field | Notes |
|-------|-------|
| `riders` | List of `RiderLite` in the group |
| `bucket_key` | e.g. `TO LAX \| POMONA` |
| `suggested_time_iso` | Pickup datetime (ISO) after time rules |
| `group_subsidy` | Set in `main.apply_group_subsidy` |
| `group_voucher` | Set by dry-run CSV assignment; production vouchers are assigned in the RPC |
| `ride_type` | e.g. `"Connect"` for shuttles; else Uber sizing applies |

---

## Supabase tables

### `Flights` (read + update)

**Read** by `rider_data.fetch_flights()` for the CLI date window. Rows with `matched = true` are excluded client-side.

| Field (used) | Role |
|--------------|------|
| `flight_id` | Unique signup |
| `user_id` | Join to `Users` |
| `date` | Filtered by `--days-ahead` / `--days-ahead-start` |
| `earliest_time`, `latest_time` | Matching windows |
| `airport`, `to_airport`, `terminal` | Bucketing + rules |
| `flight_no`, `airline_iata` | Same-flight priority |
| `bag_no`, `bag_no_large`, `bag_no_personal` | Capacity rules |
| `matched` | `true` = already matched; pipeline skips |

**Updated** in production by the `commit_matching_run` RPC:

| Update | When |
|--------|------|
| `matched = true`, `original_unmatched = false` | Flight placed in a group this run |
| `matched = false`, `original_unmatched = true` | Flight in date window but still unmatched |

Connect merge may also read matched flights when rebuilding groups from existing rides.

### `Users` (read)

| Field | Role |
|-------|------|
| `user_id` | Join key |
| `school` | Required for bucketing; missing → flight skipped |
| `firstname`, `lastname` | Combined into rider `name` for CSV |

### `Rides` (read + insert + transactional cleanup)

**Insert** (production): one row per matched group inside `commit_matching_run`.

| Field (written) | Value |
|-----------------|-------|
| `ride_date` | Date from earliest overlap logic |
| `ride_type` | `Connect` for Connect groups; otherwise null |

**Read** by `connect_merge.py` for existing rides in the run’s date range.

**Cleanup** happens inside `commit_matching_run` when small rides are merged into Connect.

### `Matches` (read + insert + transactional cleanup)

One row per rider in a group. All riders in a group share the same `ride_id`.

| Field (written) | Value |
|-----------------|-------|
| `ride_id` | From new `Rides` insert |
| `user_id`, `flight_id` | Rider identity |
| `date`, `time` | Earliest-overlap date/time (DB persist) |
| `earliest_time`, `latest_time` | Group window |
| `source` | `"ml"` |
| `voucher`, `contingency_voucher` | From `public."Vouchers"` during production RPC commit |
| `is_verified` | `false` on insert |
| `is_subsidized` | `true` only if subsidized **and** date covered |
| `uber_type` | `X` / `XL` / `XXL` or Connect |

**Delete** when Connect merge removes absorbed rides.

### `AlgorithmStatus` (read + insert + update)

Production runs only. Tracks scheduled and completed matcher executions.

| Field (used) | Role |
|--------------|------|
| `algorithm_name` | `"pickup_matching"` |
| `target` | `Pomona` / `Other` / `All` from rider schools |
| `status` | `scheduled` → `running` → `success` / `failed` |
| `scheduled_for`, `started_at`, `finished_at` | Timing |
| `run_id` | UUID per run |
| `error_message` | Set on failure |

### `MatchingRuns` (insert + update by RPC)

Idempotency ledger for production commits. `commit_matching_run` creates and
locks one row per `run_id` before making production writes.

| Field | Role |
|-------|------|
| `run_id` | Primary key and idempotency key for one production commit |
| `status` | `committing` while the transaction is active; `committed` after success |
| `payload_hash` | Hash of the commit payload; prevents reusing a `run_id` with different data |
| `commit_result` | JSON summary returned by the first successful commit |
| `started_at`, `committed_at` | Commit timing |

If the same `run_id` and payload are submitted again after a successful commit,
the RPC returns the existing `commit_result` with `idempotent_replay = true`.
If a statement fails during the RPC, Postgres rolls back the `MatchingRuns` row
and all ride/match/flight/voucher cleanup changes from that attempt.

### Integrity safeguards

`documentation/sql/002_integrity_safeguards.sql` adds defensive database
constraints and indexes for production state:

- `Matches.ride_id` must reference `Rides.ride_id`
- `Matches.flight_id` must reference `Flights.flight_id`
- `Vouchers.used_by_run_id` must reference `MatchingRuns.run_id`
- used vouchers must have a `used_by_run_id`
- required match fields must be present
- each flight may have at most one active `Matches` row

The file includes diagnostic queries that should return no rows before applying
the constraint/index section.

---

## Local CSV outputs

Written from `Algorithm/` with default paths under `../matches/`.

### `matches_dryrun.csv` (matched groups)

One row per group. Key columns:

| Column | Meaning |
|--------|---------|
| `ride_id_simulated` | Sequential id for review (not DB `ride_id`) |
| `bucket_key` | Bucket label |
| `date` | Group ride date |
| `num_riders` | Group size |
| `suggested_time` | Pickup time after 15-min rules |
| `earliest_time`, `latest_time` | Group overlap window |
| `match_times` | Per-rider windows (JSON-ish list string) |
| `considered_bags`, `bags_total` | Bag units / counts |
| `riders` | JSON list of names |
| `voucher`, `contingency_vouchers` | Assigned codes |
| `subsidized` | Group subsidy flag |
| `uber_type` | Vehicle size or `Connect` |

### `unmatched_reasons_dryrun.csv`

One row per unmatched rider in the run window.

| Column | Meaning |
|--------|---------|
| `flight_id`, `user_id`, `name` | Identity |
| `airport`, `to_airport`, `date`, times | Signup details |
| `bucket_key` | Bucket they were in |
| `reason` | Primary blocker (see below) |
| `details` | Extra context from `audit.py` |

**Common `reason` values:** `singleton_bucket`, `no_time_overlap`, `terminal_mismatch`, `bag_capacity`, `dominated_by_better_pair`, `unknown`.

---

## `Vouchers` (production source of truth)

One row per voucher. Production voucher assignment and consumption happens inside
`commit_matching_run`, in the same database transaction as ride/match creation,
flight updates, and Connect cleanup.

| Field | Role |
|-------|------|
| `voucher_id` | Primary key |
| `voucher_link` | Uber voucher URL/code inserted into `Matches` when assigned |
| `airport`, `to_airport`, `contingency` | Eligibility filters |
| `start_date`, `end_date` | Date eligibility window |
| `used`, `used_at` | Consumption state |
| `used_by_run_id` | Algorithm run that consumed the voucher |
| `assigned_ride_id`, `assigned_flight_id` | Ride/flight assignment audit trail |
| `import_batch_id` | Optional CSV import tracking |

## Voucher CSV

Local pool under `vouchers/*.csv`. CSV assignment is retained for dry-run/review
output and import compatibility; production voucher state should come from
`public."Vouchers"`.

Use `Algorithm/import_vouchers.py` to validate and import CSV rows into
`public."Vouchers"`. The legacy CSV `USED` column is not treated as production
consumption state during import; imported rows are made available, and
`commit_matching_run` records production consumption with `used_at`,
`used_by_run_id`, `assigned_ride_id`, and `assigned_flight_id`.

Expected columns (used by `vouchers.py`):

| Column | Role |
|--------|------|
| `Date (start)`, `Date (end)` | MM-DD validity window (year inferred from ride date) |
| `Contingency` | `true` / `false` — contingency vs group voucher |
| `voucher_link` | Code or URL assigned to rider |
| `TO_AIRPORT` | Direction filter |
| `AIRPORT` | Airport filter (`LAX`, `ONT`, …) |
| `USED` | Pool consumption; parsed safely (string `"False"` ≠ used) |

Dry-run writes to `<original>.dryrun.csv` instead of mutating the source file.

---

## Next steps

- [Operations](operations.md) — run and verify against this data  
- [Matching rules](matching-rules.md) — what drives grouping decisions  
- [Code guide](code-guide.md) — which module reads/writes each piece
