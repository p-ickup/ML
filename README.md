# Pickup Matching Pipeline

Batch job that groups Pomona students into shared airport rides. Reads unmatched flight signups from Supabase, forms Uber groups (2–5) and optional Connect shuttles (6–24), applies subsidy and vouchers, writes CSV or persists to the database.

**Entry point:** `Algorithm/main.py`

---

## Quick start

```bash
# From repo root
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Add SUPABASE_URL and SUPABASE_SECRET_KEY to .env

cd Algorithm
python3 main.py --dry-run --days-ahead 20 --vouchers ../vouchers/Summer.csv
```

Review `../matches/matches_dryrun.csv` before running without `--dry-run`.

Run tests (stdlib `unittest`, no extra deps) from the repo root:

```bash
python3 -m unittest discover -s tests -t .
```

---

## Documentation

Full onboarding lives in **[documentation/](documentation/README.md)** — bite-sized guides:

| Doc | Topic |
|-----|--------|
| [documentation/README.md](documentation/README.md) | **Start here** — reading order and index |
| [Platform overview](documentation/platform-overview.md) | What the system does and how it fits Pickup |
| [Code guide](documentation/code-guide.md) | Repository layout and what each file does |
| [Schema & data](documentation/schema.md) | Supabase tables, CSV columns, in-memory types |
| [Operations](documentation/operations.md) | Install, run, verify, troubleshoot |
| [Matching rules](documentation/matching-rules.md) | Buckets, Connect, subsidy, vouchers |
| [Glossary](documentation/glossary.md) | Term definitions |
| [Pipeline diagram](documentation/pipeline_diagram.html) | Interactive flow (browser) |

---

## Common commands

Run from **`Algorithm/`**:

```bash
# Dry-run (review only)
python3 main.py --dry-run --days-ahead 20 --vouchers ../vouchers/Summer.csv

# Production
python3 main.py --days-ahead 20 --vouchers ../vouchers/Summer.csv
```

| Flag | Purpose |
|------|---------|
| `--dry-run` | No DB writes; voucher `.dryrun` copy |
| `--days-ahead N` | Include flights through today + N days |
| `--days-ahead-start N` | Start of window (use `0` to include today) |
| `--vouchers PATH` | Voucher CSV |
| `--csv PATH` | Output CSV path |

Details: [Operations](documentation/operations.md).

---

## Repository layout

```text
Algorithm/        Pipeline code — run main.py from here
documentation/    Onboarding docs + pipeline diagram
matches/          Dry-run CSV output
vouchers/         Voucher pools
requirements.txt
.env              Supabase credentials (not committed)
```

---

## Related

| Resource | Use |
|----------|-----|
| [SBOM.spdx](SBOM.spdx) | Dependency inventory + licenses (SPDX, FOSSA-generated) |

**Note:** Root `Dockerfile` targets a legacy web API, not this batch pipeline. Use `python3 main.py` directly.
