# Repository & code guide

## Repository layout

```text
ML/
├── Algorithm/                 # All pipeline Python (run commands from here)
│   ├── main.py                # Entry point
│   ├── config.py              # Policy knobs — edit first
│   ├── rider_data.py          # Supabase intake
│   ├── buckets.py             # Bucket assignment
│   ├── ruleMatching.py        # Core matcher
│   ├── audit.py               # Unmatched reasons
│   ├── connect_policy.py      # Connect on/off from config
│   ├── connect_merge.py       # Connect merge with DB
│   ├── vouchers.py            # Voucher assignment
│   └── algorithmStatus.py     # Production run tracking
├── tests/                       # unittest suite (stdlib only)
├── documentation/             # Onboarding docs + pipeline diagram
├── matches/                     # Dry-run CSV output
├── vouchers/                    # Voucher pool CSVs
├── requirements.txt
├── .env                         # Supabase credentials (not committed)
└── README.md                    # Quick start → links here
```

Run unit tests from the repo root: `python3 -m unittest discover -s tests -t .`.
Run live Supabase integration tests explicitly: `python3 -m unittest tests.integration_supabase`
(see [Operations → Tests](operations.md#tests)). The integration suite touches
the configured Supabase database and covers voucher import, `AlgorithmStatus`,
production commit side effects, rollback, and selected `main.run(...)`
success and failure lifecycle scenarios.
Integration helpers live in `tests/integration_supabase_base.py`; direct RPC
coverage lives in `tests/integration_supabase_commit.py`; pipeline lifecycle
coverage lives in `tests/integration_supabase_pipeline.py`.

**Not used for current pipeline:** root `Dockerfile` targets a legacy `MLapi` web app. Run `python3 main.py` directly.

---

## Python modules

### `main.py`

**Role:** CLI entry and orchestrator.

**Does:**

- Loads `.env`, creates Supabase client  
- Calls fetch → bucket → match → post-process → Connect merge → times → subsidy
- Writes dry-run CSVs, or calls the transactional DB commit RPC in production
- Updates `AlgorithmStatus` for production runs
- Defines `apply_group_subsidy()` and LAX connect retry helper  

**Start here when:** changing run order, subsidy thresholds, or DB/CSV write behavior.

---

### `config.py`

**Role:** Single policy file for matching, Connect, and covered dates.

**Key settings:** bag limits, `MAX_GROUP_SIZE`, terminal mode, overlap grace, `CONNECT_*`, `COMPATIBLE_SCHOOLS`, `COVERED_DATES_*`.

**Start here when:** ops asks to change limits, airports, Connect tiers, or covered break dates — **before** editing matcher code.

---

### `rider_data.py`

**Role:** Supabase → `RiderLite`.

**Does:**

- `fetch_flights()` — date window + skip `matched = true`  
- `fetch_users()` — school and name  
- Normalizes airport and terminal strings  

**Start here when:** changing who gets loaded or how flight/user rows are joined.

---

### `buckets.py`

**Role:** Split riders into disjoint buckets before matching.

**Bucket key format:** `TO LAX | POMONA`, `FROM ONT | ALL`, etc.

**Start here when:** adding schools or changing who can match with whom (with `COMPATIBLE_SCHOOLS` in config).

---

### `ruleMatching.py`

**Role:** Core matching engine (largest module).

**Does:**

- `match_bucket()` — pairs, expansion, leftovers, optimizations  
- In-bucket Connect attempts  
- `_ont_post_process_unmatched()`  
- `refresh_match_suggested_times()` — 15 min before/after overlap  
- Defines `Match` dataclass  

**Start here when:** changing scoring, group validity, or pickup time logic.

---

### `audit.py`

**Role:** Explain why riders did not match.

**Does:**

- `pair_block_reason()` — overlap, terminal, bags  
- Builds per-rider `reason` for `unmatched_reasons_dryrun.csv`  

**Start here when:** improving unmatched diagnostics or adding new block reasons.

---

### `connect_policy.py`

**Role:** Read `CONNECT_*` from config; answer “is Connect enabled for this airport/direction/size?”

**Start here when:** adding Connect rules without duplicating config parsing in other files.

---

### `connect_merge.py`

**Role:** Post-match Connect merge across **this run + existing Supabase rides + unmatched**.

**Does:**

- Loads `Rides` / `Matches` / `Flights` for date range  
- Forms Connect-tier groups
- Returns cleanup intent for absorbed smaller rides
- Production cleanup is committed transactionally by the `commit_matching_run` RPC

**Start here when:** changing how Connect combines with historical matches.

---

### `commit_payload.py`

**Role:** Build and validate the production commit payload sent to Supabase.

**Does:**

- Validates commit invariants before production persistence
- Builds one payload containing groups, match rows, voucher eligibility, unmatched flight updates, and Connect cleanup intent
- Calls the `commit_matching_run` RPC, which is the production transaction boundary
- Uses `run_id` as the idempotency key for production commits

**Start here when:** changing production persistence fields or commit invariants.

**Database side:** `documentation/sql/001_commit_matching_run.sql` defines
`public."MatchingRuns"` and `public.commit_matching_run(...)`. The RPC records
successful commits by `run_id`; retrying the same payload returns the prior
commit result instead of duplicating writes. Failed RPC attempts roll back in
Postgres.

Additional safeguards live in `documentation/sql/002_integrity_safeguards.sql`.
Run its diagnostic queries before applying the constraints/indexes.

---

### `vouchers.py`

**Role:** Assign group and contingency vouchers from a local CSV pool for dry-run/review workflows.

**Rules:** no Connect vouchers; subsidized + covered only; safe `USED` parsing.

**Start here when:** changing dry-run voucher output or CSV column handling. Production voucher consumption happens in the `commit_matching_run` RPC against `public."Vouchers"`.

---

### `import_vouchers.py`

**Role:** Validate legacy voucher CSVs and import them into `public."Vouchers"`.

**Does:**

- Validates required CSV columns and date ranges
- Maps CSV rows to the `Vouchers` table shape
- Upserts rows by `voucher_link`
- Defaults imported rows to available production vouchers

**Start here when:** changing the admin/import workflow for voucher CSVs.

---

### `algorithmStatus.py`

**Role:** Production run lifecycle in `AlgorithmStatus` table.

**Does:** pick scheduled run or create new; mark `running` / `success` / `failed`.

---

## I want to…

| Goal | File(s) |
|------|---------|
| Run the pipeline | `main.py` (CLI) |
| Change bag / group size / overlap | `config.py` |
| Turn Connect on/off | `config.py` → `connect_policy.py` reads it |
| Change Connect merge behavior | `connect_merge.py` |
| Change who gets loaded from DB | `rider_data.py` |
| Add a school | `config.py` + `buckets.py` |
| Change pairing/scoring | `ruleMatching.py` |
| Change subsidy thresholds | `main.py` (`apply_group_subsidy`) |
| Change covered subsidy dates | `config.py` |
| Fix voucher assignment | `vouchers.py` |
| Understand unmatched CSV reasons | `audit.py` |
| Track production runs | `algorithmStatus.py` |

---

## Dependencies

From `requirements.txt`:

| Package | Used for |
|---------|----------|
| `pandas` | Voucher CSV handling |
| `python-dotenv` | `.env` loading in `main.py` |
| `supabase` | Supabase database client |

---

## Next steps

- [Schema & data](schema.md) — fields this code reads/writes  
- [Operations](operations.md) — run from `Algorithm/`  
- [Matching rules](matching-rules.md) — policy detail
