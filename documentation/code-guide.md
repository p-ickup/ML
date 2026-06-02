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

Run tests from the repo root: `python3 -m unittest discover -s tests -t .` (see [Operations → Tests](operations.md#tests)).

**Not used for current pipeline:** root `Dockerfile` targets a legacy `MLapi` web app. Run `python3 main.py` directly.

---

## Python modules

### `main.py`

**Role:** CLI entry and orchestrator.

**Does:**

- Loads `.env`, creates Supabase client  
- Calls fetch → bucket → match → post-process → Connect merge → times → subsidy → vouchers  
- Writes CSV; writes DB + `AlgorithmStatus` in production  
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
- Forms Connect-tier groups; may delete absorbed smaller rides  
- `cleanup_absorbed_rides()` before DB write  

**Start here when:** changing how Connect combines with historical matches.

---

### `vouchers.py`

**Role:** Assign group and contingency vouchers from a local CSV pool.

**Rules:** no Connect vouchers; subsidized + covered only; safe `USED` parsing.

**Start here when:** changing voucher selection or CSV column handling.

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
