# Pickup Matching Pipeline

This repository contains the ride matching pipeline used by Pickup. The main script is `Algorithm/main.py`.

The pipeline reads unmatched flight forms from Supabase, turns them into lightweight rider records, buckets riders by direction, airport, and school compatibility, forms ride groups, applies subsidy and voucher rules, writes CSV reports, and writes final matches back to Supabase when not running in dry-run mode.

## Setup

Run from the project root:

```bash
cd /Users/xmora/Documents/Pickup/ML
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The matcher expects Supabase credentials in `.env`:

```bash
SUPABASE_URL=your_supabase_url
SUPABASE_SECRET_KEY=your_supabase_secret_key
```

`SUPABASE_SECRET_KEY` is the key used by `Algorithm/main.py`.

## Running The Main Pipeline

Run the matcher from the `Algorithm` folder because the default file paths are relative to that folder. From the project root:

```bash
cd Algorithm
```

Standard dry-run command:

```bash
python3 main.py --dry-run --days-ahead 20 --vouchers ../vouchers/Summer.csv
```

Production run:

```bash
python3 main.py --vouchers ../vouchers/SpringBreak.csv
```

Dry runs are for checking the generated groups before writing matches to Supabase. Production runs create `Rides` rows, insert `Matches` rows, update `Flights.matched`, update `Flights.original_unmatched`, and update `AlgorithmStatus`.

## Main Flags

`--dry-run`

Runs the full pipeline without writing final matches to Supabase. It still fetches live Supabase data, performs matching, assigns vouchers in memory, writes CSV outputs, and creates a local voucher `.dryrun.csv` copy so real vouchers are not consumed.

Example:

```bash
python3 main.py --dry-run --vouchers ../vouchers/SpringBreak.csv
```

`--csv PATH`

Sets the matched-groups CSV output path.

Default:

```bash
../matches/matches_dryrun.csv
```

The code writes this CSV in both dry-run and production runs. The flag name says dry-run output, but the CSV writer is called before the production database write as well.

Example:

```bash
python3 main.py --dry-run --csv ../matches/springbreak_check.csv --vouchers ../vouchers/SpringBreak.csv
```

`--days-ahead N`

Sets the inclusive end of the flight date window. The matcher reads flights on or before today's date plus `N` days.

Default:

```bash
10
```

Example:

```bash
python3 main.py --dry-run --days-ahead 30 --vouchers ../vouchers/SpringBreak.csv
```

`--days-ahead-start N`

Sets the inclusive start of the flight date window. The matcher reads flights on or after today's date plus `N` days.

If this flag is omitted, the legacy lower bound is used: flight date must be strictly after today. Use `--days-ahead-start 0` if you want to include today.

`--days-ahead-start` cannot be greater than `--days-ahead`.

Example:

```bash
python3 main.py --dry-run --days-ahead-start 0 --days-ahead 14 --vouchers ../vouchers/SpringBreak.csv
```

`--vouchers PATH`

Sets the local voucher CSV used by the run.

Default in code:

```bash
../vouchers/Thanksgiving.csv
```

Pass this explicitly unless the default file exists and is the intended voucher pool.

Example:

```bash
python3 main.py --dry-run --vouchers ../vouchers/WinterReturn.csv
```

When `USE_SUPABASE_STORAGE = False`, production runs update the local voucher CSV passed through this flag. When `USE_SUPABASE_STORAGE = True`, production runs use the configured Supabase Storage active voucher file instead.

## Data Read By The Pipeline

`RiderData.fetch_flights()` reads from `Flights` where `matched` is `NULL`.

The selected flight fields are:

```text
flight_id, user_id, flight_no, airline_iata, earliest_time, latest_time,
airport, date, to_airport, terminal, matched, bag_no, bag_no_large,
bag_no_personal
```

The date window comes from `--days-ahead` and `--days-ahead-start`.

`RiderData.fetch_users()` reads `user_id`, `school`, `firstname`, and `lastname` from `Users`. A flight is skipped if its user has no school value, because school is required for bucketing.

Each valid flight becomes a `RiderLite` object with normalized airport, normalized terminal, school, name, flight window, bag counts, direction, and flight identifiers.

## Pipeline Flow

1. `main.py` loads `.env`, creates the Supabase client, and starts `run()`.
2. `rider_data.py` fetches candidate flights and user records.
3. `buckets.py` groups riders by direction, airport, and school compatibility.
4. `force_match.py` optionally creates one forced match if `NEPO_MODE` is enabled.
5. `ruleMatching.py` runs the core matching logic inside each bucket.
6. `main.py` runs cross-bucket post-processing for ONT and LAX.
7. `main.py` applies subsidy rules.
8. `vouchers.py` assigns group and contingency vouchers after final groups are known.
9. `main.py` writes matched and unmatched CSV reports.
10. If this is not a dry run, `main.py` writes final groups to Supabase and updates algorithm status.

## Bucketing

Buckets are created in `Algorithm/buckets.py`.

Each bucket key looks like:

```text
TO LAX | POMONA
FROM ONT | ALL
```

The direction is `TO` when `to_airport` is true and `FROM` when it is false. The airport comes from the flight record. The school group comes from `COMPATIBLE_SCHOOLS` in `config.py`.

Current compatibility behavior:

```python
COMPATIBLE_SCHOOLS = {
    "POMONA": ["POMONA"],
}
```

That means Pomona riders only bucket with Pomona riders. Schools not listed in `COMPATIBLE_SCHOOLS` go into `ALL` and can match with each other.

## Core Matching Logic

The core engine is `Algorithm/ruleMatching.py`.

For each bucket, the matcher:

1. Handles LAX outbound Connect shuttles first when the bucket is `TO LAX` and enough riders exist.
2. Sorts riders by shortest time window first so constrained riders are considered earlier.
3. Builds all feasible rider pairs.
4. Scores feasible pairs.
5. Selects the best non-overlapping pairs.
6. Expands each pair greedily up to `MAX_GROUP_SIZE`.
7. Runs leftover passes and optimization passes.
8. Returns matched groups, unmatched riders, and diagnostics.

### Group Validity

A normal group must:

1. Have between 2 and `MAX_GROUP_SIZE` riders.
2. Have overlapping time windows, or be within the allowed touching/grace rules.
3. Fit bag limits.
4. Match terminal exactly only when `TERMINAL_MODE = "strict"`.

Bag logic uses:

```python
MAX_TOTAL_BAGS = 10
MAX_LARGE_BAGS = 5
MAX_GROUP_SIZE = 5
PERSONAL_CONSTRAINT = False
LARGE_BAG_MULTIPLIER = 2
```

Large bags count as `LARGE_BAG_MULTIPLIER` bag units. Personal bags only count when `PERSONAL_CONSTRAINT` is true.

Special bag behavior:

1. Groups of 5 must stay under the tighter bag limit used in `ruleMatching.py`.
2. Groups of 3 may allow up to 12 bag units.
3. The LAX final retry can relax group-of-3 bag handling so some XXL-style groups can still form.

### Time Overlap

Time windows come from each rider's `earliest_time` and `latest_time`.

`ALLOW_TOUCHING = True` allows windows that just touch. `OVERLAP_GRACE_MIN` allows small negative gaps to count as touching. `TOUCH_FLOOR_MIN` gives touching windows a small score floor so they can still be compared.

Same-flight matching can be more forgiving when `SAME_FLIGHT_PRIORITY = True`. Riders on the same airline, flight number, and date can be matched even when their windows are not as clean, up to the same-flight grace behavior in `ruleMatching.py`.

### Scoring

Higher score is better.

The score mainly rewards usable overlap minutes. It also adds a small bag-fill bonus, adds a same-flight bonus, and applies a large penalty for terminal mismatch in relaxed terminal mode.

When `SAME_FLIGHT_PRIORITY = True`, a same-flight group receives a very large score bonus so it wins over ordinary matches.

### Suggested Ride Time

Suggested time is created in `_group_to_match()`.

For rides going to the airport, the chosen time is 15 minutes before the end of the shared overlap, if that still fits inside the overlap.

For rides coming from the airport, the chosen time is 15 minutes after the start of the shared overlap, if that still fits inside the overlap.

If the overlap only touches, the chosen time falls back to the overlap start.

## Leftover And Optimization Passes

After the first greedy match pass, `ruleMatching.py` tries to improve the result:

`_second_pass_leftovers()`

Forms new groups from riders left unmatched by the first pass.

`_split_full_group_for_leftovers()`

When `SPLIT_4_TO_3_2 = True`, tries to split a full group so a leftover rider can be rescued.

`_third_pass_absorb_leftovers()`

When `ABSORB_LEFTOVERS = True`, tries to insert leftovers into existing valid groups.

`_promote_lax_twos()`

For LAX buckets, tries to turn 2-person groups into 3-person groups by moving one rider from another group, while keeping both groups valid.

`_combine_pairs_into_fours()`

Tries to combine two 2-person groups into one 4-person group when valid.

`_lax_optimize_4_and_2()`

For LAX and ONT buckets, tries to improve a 4-person plus 2-person result into two 3-person groups.

`_final_leftovers()`

Tries one final placement of remaining leftovers into existing groups that still have capacity.

## LAX Connect Shuttle Logic

Connect shuttles are only for LAX departures, meaning `TO LAX` buckets.

The thresholds are set in `config.py`:

```python
LAX_CONNECT_SHUTTLE_MIN = 8
LAX_CONNECT_SHUTTLE_TIER1_MAX = 25
LAX_CONNECT_SHUTTLE_TIER2_MIN = 30
LAX_CONNECT_SHUTTLE_TIER2_MAX = 55
```

The Connect logic uses time overlap and ignores bag capacity. It first tries to form large Connect groups directly inside the LAX bucket. After normal matching, `merge_groups_into_connect_shuttles()` tries to merge overlapping LAX departure groups into a Connect shuttle when the combined size fits a Connect tier.

Connect matches get `ride_type = "Connect"` and do not receive vouchers.

## ONT And LAX Post-Processing

After all buckets finish, `main.py` runs two post-processing steps:

`_ont_post_process_unmatched()`

Looks at unmatched ONT riders and existing ONT groups of 4 on the same day. If it can move one rider out of a 4-person group and pair that rider with an unmatched rider, it creates a 3-person group plus a 2-person group.

`_final_lax_unmatched_retry()`

When Connect shuttle mode is enabled, unmatched LAX riders are re-bucketed and matched one more time. This can form new LAX groups from riders who were left unmatched earlier.

## Subsidy Logic

Subsidy is applied in `main.py` after all matching and post-processing is complete.

Current thresholds:

```python
{"LAX": 3, "ONT": 2}
```

A group qualifies only when:

1. The airport has a threshold.
2. The group size is at least that threshold.
3. Every rider in the group is Pomona.
4. The ride date is covered by the configured outbound or inbound covered dates.

The covered dates live in `config.py`:

```python
COVERED_DATES_EXPLICIT = True
COVERED_DATES_OUTBOUND = ["03-13", "03-14", "03-15"]
COVERED_DATES_INBOUND = ["03-20", "03-21", "03-22"]
```

If `COVERED_DATES_EXPLICIT` is true, a group is not subsidized unless its date is in the correct list for its direction.

## Voucher Logic

Voucher assignment lives in `Algorithm/vouchers.py` and happens after final groups and subsidy status are known.

Voucher CSVs are expected to contain columns like:

```text
Date (start), Date (end), Contingency, voucher_link, TO_AIRPORT, AIRPORT, USED
```

Rules:

1. Connect shuttles do not receive vouchers.
2. Group vouchers are only assigned to subsidized, covered groups.
3. Contingency vouchers are only assigned for inbound riders in subsidized, covered groups.
4. Dry runs work on a `.dryrun.csv` copy so the original voucher file is not consumed.
5. Production runs either update the local voucher CSV or use Supabase Storage, depending on `USE_SUPABASE_STORAGE`.

## Output Files

`../matches/matches_dryrun.csv`

Matched groups. Includes simulated ride id, bucket, date, rider count, suggested time, shared time window, bag counts, rider names, voucher values, subsidy status, and Uber type.

`../matches/unmatched_reasons_dryrun.csv`

Unmatched riders and the best available reason. Reasons come from `audit.py` and include cases like `singleton_bucket`, `no_time_overlap`, `terminal_mismatch`, `bag_capacity`, and `dominated_by_better_pair`.

`../vouchers/*.dryrun.csv`

Voucher working copy created during dry runs.

## Algorithm Folder Guide

`Algorithm/main.py`

The main orchestration file. It parses CLI flags, connects to Supabase, fetches riders, calls bucketing and matching, runs ONT/LAX post-processing, applies subsidies, assigns vouchers, writes CSV reports, and writes to Supabase in production runs.

`Algorithm/rider_data.py`

The Supabase intake layer. It reads candidate rows from `Flights`, reads school/name data from `Users`, normalizes terminal and airport values, skips users without school data, and returns `RiderLite` objects.

`Algorithm/buckets.py`

Creates matching buckets. Riders are separated by direction, airport, and school compatibility before matching starts.

`Algorithm/ruleMatching.py`

The core matcher. It validates groups, scores pairs and groups, expands pairs, handles leftovers, optimizes group shapes, creates suggested ride times, handles LAX Connect formation, and returns `Match` objects.

`Algorithm/audit.py`

Explains why unmatched riders did not match. It checks pair-level blockers like time overlap, terminal mismatch, and bag capacity, then produces the reason written into `unmatched_reasons_dryrun.csv`.

`Algorithm/config.py`

The main control file for operational policy: bag limits, max group size, terminal mode, overlap grace, leftover behavior, same-flight priority, Connect shuttle thresholds, forced matching settings, school compatibility, covered dates, and storage settings.

`Algorithm/vouchers.py`

Loads the voucher pool, checks whether a ride date is covered, assigns group vouchers and contingency vouchers, avoids vouchers for Connect shuttles, and updates either a local CSV or Supabase Storage depending on config.

`Algorithm/storage.py`

Helper functions for Supabase Storage. Used when voucher or dry-run CSV files are stored in Supabase buckets instead of only local files.

`Algorithm/algorithmStatus.py`

Tracks production algorithm runs in Supabase. It determines whether the run targets Pomona, Other, or All riders, creates or reuses an `AlgorithmStatus` row, and marks the run as success or failed.

`Algorithm/force_match.py`

Optional forced matching path. When `NEPO_MODE = True`, it tries to force the configured `NEPO_USER_IDS` into a match if they are in the same bucket and the group still passes validation.

`Algorithm/temp_merge_existing_to_connect.py`

Maintenance script, not part of the normal `main.py` run. It looks at already-created LAX outbound rides in Supabase and merges eligible groups into Connect shuttles.

`Algorithm/temp_reverify_group_times.py`

Maintenance script, not part of the normal `main.py` run. It recalculates suggested times for existing rides and updates Supabase when times differ.
