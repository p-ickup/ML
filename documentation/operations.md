# Operations

How to install, run, verify, and troubleshoot the matching pipeline.

## Prerequisites

- Python 3.9+ (3.11+ recommended; repo tested on 3.13)
- Supabase credentials with service-role access for the transactional commit RPC
- Applied SQL from `documentation/sql/001_commit_matching_run.sql`
- Applied integrity safeguards from `documentation/sql/002_integrity_safeguards.sql` after diagnostics are clean
- Voucher rows imported into `public."Vouchers"` for production voucher assignment
- Voucher CSV for dry-run review output, if reviewing vouchers locally

---

## Setup

From the **repository root**:

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Create `.env` at the repo root:

```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SECRET_KEY=your_service_role_or_secret_key
```

`main.py` loads `.env` via `python-dotenv` when you run from `Algorithm/`.

---

## First run

Always **`cd Algorithm`** — paths like `../vouchers/` are relative to that folder.

```bash
cd Algorithm
python3 main.py --dry-run --days-ahead 20 --vouchers ../vouchers/Summer.csv
```

Expected console output:

```text
Fetched N rider forms (dates: … → …)
Buckets: X | Groups: Y | Unmatched: Z
Wrote dry-run CSV with Y matched groups → ../matches/matches_dryrun.csv
```

Then open:

1. `../matches/matches_dryrun.csv` — review groups, times, subsidy, vouchers  
2. `../matches/unmatched_reasons_dryrun.csv` — scan `reason` column  

Interactive flow: [pipeline_diagram.html](pipeline_diagram.html).

---

## Standard commands

| Goal | Command |
|------|---------|
| Dry-run (default for review) | `python3 main.py --dry-run --days-ahead 20 --vouchers ../vouchers/Summer.csv` |
| Production | `python3 main.py --days-ahead 20` |
| Include today in window | add `--days-ahead-start 0` |
| Custom dry-run CSV path | `--csv ../matches/my_review.csv` |

### Example: break prep (30-day window)

```bash
python3 main.py --dry-run --days-ahead-start 0 --days-ahead 30 \
  --vouchers ../vouchers/Summer.csv
```

Review CSVs → fix config if needed → re dry-run → production without `--dry-run`.

---

## CLI reference

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | off | No production writes; writes review CSVs and voucher `.dryrun` copy |
| `--csv PATH` | `../matches/matches_dryrun.csv` | Dry-run matched groups CSV |
| `--days-ahead N` | `10` | Flights with `date` ≤ today + N |
| `--days-ahead-start N` | *(omit)* | Flights with `date` ≥ today + N. Omit = exclude today. Use `0` for today |
| `--vouchers PATH` | `../vouchers/Thanksgiving.csv` | Dry-run voucher CSV pool |

`--days-ahead-start` must be ≤ `--days-ahead`.

---

## Dry-run vs production

| | Dry-run | Production |
|--|---------|------------|
| Read `Flights` / `Users` | Yes | Yes |
| Write `Rides` / `Matches` | **No** | **Yes** |
| Update `Flights.matching_status` | **No** | **Yes** |
| Voucher source | Local CSV `*.dryrun.csv` copy | `public."Vouchers"` table |
| `AlgorithmStatus` | Skipped | Updated |
| CSV output | Yes | No |

**Production side effects:**

- Calls `commit_matching_run` once with a validated payload
- Inserts `Rides` and `Matches`
- Sets matched flights to `matching_status='matched'`
- Marks still-unmatched flights `matching_status='unmatched'`, `original_unmatched=true`
- Cleans up rides absorbed into Connect merge
- Consumes vouchers from `public."Vouchers"`
- Records the commit in `public."MatchingRuns"` for idempotent retries

---

## Pre-production checklist

- [ ] Dry-run completed for the intended date window  
- [ ] `matches_dryrun.csv` reviewed (group sizes, times, airports)  
- [ ] Subsidy and vouchers look correct for covered dates  
- [ ] `config.py` has correct `COVERED_DATES_*` and `CONNECT_*` for this break  
- [ ] `public."Vouchers"` has enough unused rows for expected subsidized groups
- [ ] `commit_matching_run` and integrity safeguard SQL have been applied
- [ ] Stakeholders signed off on unmatched count  

---

## Config before a break

Edit `Algorithm/config.py` (typical ops tasks):

| Setting | Purpose |
|---------|---------|
| `COVERED_DATES_OUTBOUND` / `INBOUND` | MM-DD lists for subsidy/vouchers |
| `CONNECT_DEPARTURE` / `CONNECT_ARRIVAL` | Which airports/directions get Connect |
| `CONNECT_SIZE1` / `CONNECT_SIZE2` | Connect tier sizes |
| `COMPATIBLE_SCHOOLS` | Who matches together |

Subsidy **size** thresholds (`LAX: 3`, `ONT: 2`) are in `main.py` → `apply_group_subsidy`, not `config.py`.

After config changes, always re dry-run.

---

## Output files

| Path | When |
|------|------|
| `matches/matches_dryrun.csv` | Dry-run only, unless `--csv` overrides |
| `matches/unmatched_reasons_dryrun.csv` | Dry-run only, when there are unmatched riders |
| `vouchers/<name>.dryrun.csv` | Dry-run only |

Column definitions: [Schema → CSV outputs](schema.md#local-csv-outputs).

---

## Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `Fetched 0 rider forms` | Narrow date window or all flights already matched | Widen `--days-ahead`; check `Flights.matching_status` in Supabase |
| Many `no_time_overlap` | Sparse or incompatible windows | Normal for low volume; verify form times in DB |
| Many `singleton_bucket` | Only one rider in bucket | Need more signups for that airport/day |
| No vouchers assigned in dry-run | Not subsidized, wrong date, or Connect | Check `subsidized` column + `COVERED_DATES_*` |
| No vouchers assigned in production | No eligible unused row in `public."Vouchers"` | Check airport/direction/date/contingency fields in `Vouchers` |
| Connect groups missing | Connect disabled or below min size | Check `CONNECT_*` in config |
| Pickup times look wrong | Group changed after Connect merge | Times refreshed post-merge; check `suggested_time` in CSV |
| Production error mid-run | Supabase / data issue | Read console + `AlgorithmStatus.error_message`; dry-run again |

**Unmatched reasons** come from `audit.py`. Full list in [Schema](schema.md#unmatched_reasons_dryruncsv).

---

## Voucher and CSV files

- Dry-run vouchers: `--vouchers ../vouchers/YourBreak.csv`
- Dry-run writes a `*.dryrun.csv` copy; it does not touch the source pool
- Production vouchers come from `public."Vouchers"` inside the `commit_matching_run` RPC

### Import vouchers into Supabase

Validate a voucher CSV without writing to Supabase:

```bash
python3 Algorithm/import_vouchers.py vouchers/SpringBreak.csv
```

Expected output:

```text
Validated 4400 voucher rows for public.Vouchers.
Dry run only. Re-run with --commit to write to Supabase.
```

Commit the import:

```bash
python3 Algorithm/import_vouchers.py vouchers/SpringBreak.csv --commit
```

The importer validates required columns, normalizes airport codes, maps rows to
`public."Vouchers"`, and inserts only voucher links that do not already exist.
Existing voucher rows are left untouched so re-importing a CSV cannot reset
`used`, assignment, or audit fields. Newly imported vouchers are marked
available; production consumption is recorded later by `commit_matching_run`.

### Test Supabase integration writes

`tests/integration_supabase.py` is the DB-touching integration suite entrypoint.
It keeps the command below stable while loading focused tests from
`tests/integration_supabase_commit.py` and
`tests/integration_supabase_pipeline.py`. Shared live DB setup and cleanup live
in `tests/integration_supabase_base.py`.

The suite uses `vouchers/TestImport.csv` and temporary
auth/users/flights/vouchers/rides/matches rows to verify the production write
path, `AlgorithmStatus`, rollback behavior, and selected `main.run(...)`
production lifecycle success and failure scenarios.

```bash
python3 -m unittest tests.integration_supabase
```

This suite does not skip. It fails clearly if `.env` is missing Supabase
credentials, the service key cannot create temporary auth users, or the required
RPC/schema changes have not been applied. Successful runs clean up the rows they
created.

---

## Tests

The suite uses Python's built-in `unittest` — **no extra dependencies** beyond
`requirements.txt`. Run from the **repository root**:

```bash
python3 -m unittest discover -s tests -t .
```

Run lint checks from the **repository root**:

```bash
python3 -m ruff check .
```

Verbose, or a single module/case:

```bash
python3 -m unittest discover -s tests -t . -v
python3 -m unittest tests.test_vouchers
python3 -m unittest tests.test_rule_matching.TestPickupTime
```

What is covered (pure logic, no Supabase needed):

| Test file | Module under test |
|-----------|-------------------|
| `test_rider_data.py` | airport/terminal normalization |
| `test_buckets.py` | bucket key + school grouping |
| `test_connect_policy.py` | Connect enable/tiers/scope |
| `test_audit.py` | unmatched block reasons |
| `test_rule_matching.py` | overlap, validity, pickup time, `match_bucket` |
| `test_vouchers.py` | parsing, coverage, assignment, dry-run copy |
| `test_commit_payload.py` | commit payload invariants, RPC retry wrapper |
| `test_import_vouchers.py` | voucher CSV validation and missing-row insert mapping |

DB integration coverage, run explicitly:

```bash
python3 -m unittest tests.integration_supabase
```

| Test module | Live DB behavior covered |
|-------------|--------------------------|
| `tests.integration_supabase` | `TestImport.csv` import into `Vouchers`; direct `AlgorithmStatus` create/reuse/success/failure updates; `main.run(...)` success, no-rider, no-match, Connect no-merge, Connect merge, pre-commit failure, and commit-time failure scenarios; `commit_matching_run` ride/match insert; flight matched/unmatched updates; voucher consumption/audit fields; idempotent replay; rollback on mid-commit failure; Connect cleanup deleting old matches/rides |

`tests/helpers.py` adds `Algorithm/` to the path, builds `RiderLite` fixtures,
and provides a `patch_config(...)` context manager for temporary config
overrides. The unit suite deliberately does not import `main.py` (it builds a
Supabase client at import time), so unit tests run without credentials or
network.

---

## Next steps

- [Matching rules](matching-rules.md) — what to change in config  
- [Platform overview](platform-overview.md) — big picture  
- [Code guide](code-guide.md) — which file to edit
