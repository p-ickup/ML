# Platform overview

## What this system does

Pickup helps Pomona students share rides to and from airports (LAX, ONT, etc.). Students submit **flight forms** through the Pickup product; those forms live in Supabase as `Flights` rows.

This repository runs the **matching pipeline**: a batch Python job that:

1. Loads **unmatched** flight signups for a date window  
2. Groups riders into shared Uber rides (typically 2–5 people)  
3. Optionally forms larger **Connect** shuttles (6–24 people)  
4. Applies **subsidy** and determines voucher eligibility on covered dates  
5. Writes dry-run review CSVs or commits production results through Supabase

There is **no web server** in this repo. Operators or CI run `Algorithm/main.py` manually or on a schedule.

## How it fits in the wider platform

```text
┌─────────────────┐     signups      ┌──────────────────┐
│  Pickup app /   │ ───────────────► │    Supabase      │
│  student forms  │                  │  Flights, Users  │
└─────────────────┘                  └────────┬─────────┘
                                              │
                                              │ read unmatched
                                              ▼
                                     ┌──────────────────┐
                                     │  ML repo (here)  │
                                     │  main.py         │
                                     └────────┬─────────┘
                                              │
                         ┌────────────────────┼────────────────────┐
                         ▼                    ▼                    ▼
                  matches/*.csv      Rides / Matches /      Vouchers
                  (dry-run review)   Flights transaction    consumed in DB
```

**Upstream (outside this repo):** students submit forms; ops may schedule runs via `AlgorithmStatus`.

**This repo:** reads data, computes groups, writes matches.

**Downstream (outside this repo):** Pickup app or ops tools consume `Rides` / `Matches` for ride coordination, notifications, and fulfillment.

## Core pipeline stages

| Stage | Summary |
|-------|---------|
| **Fetch** | Unmatched `Flights` + `Users` → `RiderLite` list |
| **Bucket** | Split by direction (`TO`/`FROM`), airport, school |
| **Match** | Build 2–5 rider groups per bucket with overlap + bag rules |
| **Post-process** | ONT 4+2 splits, Connect retry, Connect merge with existing DB rides |
| **Finalize** | Pickup times, subsidy, voucher eligibility |
| **Output** | CSV for dry-run; transactional Supabase commit for production |

See [pipeline_diagram.html](pipeline_diagram.html) for the interactive version.

## Group types

| Type | Size | Notes |
|------|------|-------|
| **Normal Uber group** | 2–5 | Bag and terminal rules apply; may get subsidy + vouchers |
| **Connect shuttle** | 6–24 (configurable) | Time overlap only for sizing; no vouchers; `ride_type = "Connect"` |

## Configuration vs code

| Change | Usually edit |
|--------|----------------|
| Date window for a run | CLI `--days-ahead` flags |
| Bags, overlap, Connect airports, covered dates | `Algorithm/config.py` |
| Subsidy size thresholds (LAX 3, ONT 2) | `Algorithm/main.py` → `apply_group_subsidy` |
| Scoring / matching algorithm | `Algorithm/ruleMatching.py` |

Policy changes should start in `config.py` before changing matcher code.

## Production safety model

- **Dry-run** uses live reads but does **not** insert `Rides`/`Matches` or mark flights matched.  
- Vouchers in dry-run use a `*.dryrun.csv` copy so the real pool is untouched.  
- **Production** calls `commit_matching_run`, which commits rides, matches, flight updates, voucher consumption, and Connect cleanup in one transaction.
- Always dry-run and review CSVs before removing `--dry-run`.

Details: [Operations](operations.md).

## Next steps

- [Repository & code guide](code-guide.md) — files and folders  
- [Schema & data](schema.md) — tables and CSV columns  
- [Operations](operations.md) — how to run
