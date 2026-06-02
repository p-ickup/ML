# Operations

How to install, run, verify, and troubleshoot the matching pipeline.

## Prerequisites

- Python 3.9+ (3.11+ recommended; repo tested on 3.13)
- Supabase credentials with read/write access to `Flights`, `Users`, `Rides`, `Matches`, `AlgorithmStatus`
- Voucher CSV for the break (under `vouchers/`)

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
| Production | `python3 main.py --days-ahead 20 --vouchers ../vouchers/Summer.csv` |
| Include today in window | add `--days-ahead-start 0` |
| Custom CSV path | `--csv ../matches/my_review.csv` |

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
| `--dry-run` | off | No `Rides`/`Matches` writes; voucher `.dryrun` copy |
| `--csv PATH` | `../matches/matches_dryrun.csv` | Matched groups CSV |
| `--days-ahead N` | `10` | Flights with `date` ≤ today + N |
| `--days-ahead-start N` | *(omit)* | Flights with `date` ≥ today + N. Omit = exclude today. Use `0` for today |
| `--vouchers PATH` | `../vouchers/Thanksgiving.csv` | Voucher pool |

`--days-ahead-start` must be ≤ `--days-ahead`.

---

## Dry-run vs production

| | Dry-run | Production |
|--|---------|------------|
| Read `Flights` / `Users` | Yes | Yes |
| Write `Rides` / `Matches` | **No** | **Yes** |
| Update `Flights.matched` | **No** | **Yes** |
| Voucher file | `*.dryrun.csv` copy | Updates local `--vouchers` CSV |
| `AlgorithmStatus` | Skipped | Updated |
| CSV output | Yes | Yes |

**Production side effects:**

- Inserts `Rides` and `Matches`  
- Sets matched flights to `matched=true`  
- Marks still-unmatched flights `matched=false`, `original_unmatched=true`  
- May delete rides absorbed into Connect merge  
- Consumes vouchers from the local CSV passed via `--vouchers`  

---

## Pre-production checklist

- [ ] Dry-run completed for the intended date window  
- [ ] `matches_dryrun.csv` reviewed (group sizes, times, airports)  
- [ ] Subsidy and vouchers look correct for covered dates  
- [ ] `config.py` has correct `COVERED_DATES_*` and `CONNECT_*` for this break  
- [ ] Voucher CSV has enough unused rows for expected subsidized groups  
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
| `matches/matches_dryrun.csv` | Every run (unless `--csv` overrides) |
| `matches/unmatched_reasons_dryrun.csv` | When there are unmatched riders |
| `vouchers/<name>.dryrun.csv` | Dry-run only |

Column definitions: [Schema → CSV outputs](schema.md#local-csv-outputs).

---

## Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `Fetched 0 rider forms` | Narrow date window or all flights matched | Widen `--days-ahead`; check `Flights.matched` in Supabase |
| Many `no_time_overlap` | Sparse or incompatible windows | Normal for low volume; verify form times in DB |
| Many `singleton_bucket` | Only one rider in bucket | Need more signups for that airport/day |
| No vouchers assigned | Not subsidized, wrong date, or Connect | Check `subsidized` column + `COVERED_DATES_*` |
| Connect groups missing | Connect disabled or below min size | Check `CONNECT_*` in config |
| Pickup times look wrong | Group changed after Connect merge | Times refreshed post-merge; check `suggested_time` in CSV |
| Production error mid-run | Supabase / data issue | Read console + `AlgorithmStatus.error_message`; dry-run again |

**Unmatched reasons** come from `audit.py`. Full list in [Schema](schema.md#unmatched_reasons_dryruncsv).

---

## Voucher and CSV files

- Vouchers: `--vouchers ../vouchers/YourBreak.csv`  
- Dry-run: writes `*.dryrun.csv` copy; does not touch the source pool  
- Production: updates the local voucher CSV you passed  

---

## Tests

The suite uses Python's built-in `unittest` — **no extra dependencies** beyond
`requirements.txt`. Run from the **repository root**:

```bash
python3 -m unittest discover -s tests -t .
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

`tests/helpers.py` adds `Algorithm/` to the path, builds `RiderLite` fixtures,
and provides a `patch_config(...)` context manager for temporary config
overrides. The suite deliberately does not import `main.py` (it builds a
Supabase client at import time), so tests run without credentials or network.

---

## Next steps

- [Matching rules](matching-rules.md) — what to change in config  
- [Platform overview](platform-overview.md) — big picture  
- [Code guide](code-guide.md) — which file to edit
