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
| `group_voucher` | Set in `vouchers.assign_vouchers` |
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

**Updated** in production (`main._write_matches_db`):

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

### `Rides` (read + insert + delete)

**Insert** (production): one row per matched group.

| Field (written) | Value |
|-----------------|-------|
| `ride_date` | Date from earliest overlap logic |

**Read** by `connect_merge.py` for existing rides in the run’s date range.

**Delete** by `connect_merge.cleanup_absorbed_rides()` when small rides are merged into Connect.

### `Matches` (read + insert + delete)

One row per rider in a group. All riders in a group share the same `ride_id`.

| Field (written) | Value |
|-----------------|-------|
| `ride_id` | From new `Rides` insert |
| `user_id`, `flight_id` | Rider identity |
| `date`, `time` | Earliest-overlap date/time (DB persist) |
| `earliest_time`, `latest_time` | Group window |
| `source` | `"ml"` |
| `voucher`, `contingency_voucher` | From voucher assignment |
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

## Voucher CSV

Local pool under `vouchers/*.csv`.

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
