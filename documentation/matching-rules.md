# Matching rules

How riders are grouped and how subsidy/vouchers apply. Most limits live in `Algorithm/config.py`.

## Bucketing

Before matching, riders are split into **buckets** — they only match with others in the same bucket.

**Bucket key:** `{DIRECTION} {AIRPORT} | {SCHOOL_GROUP}`

Examples: `TO LAX | POMONA`, `FROM ONT | ALL`

| Part | Rule |
|------|------|
| Direction | `TO` if `to_airport=true` (school → airport); `FROM` if false |
| Airport | From flight record (`LAX`, `ONT`, …) |
| School | From `COMPATIBLE_SCHOOLS` in config; unknown schools → `ALL` |

Current config (Pomona-only):

```python
COMPATIBLE_SCHOOLS = {
    "POMONA": ["POMONA"],
}
```

---

## Normal groups (2–5 riders)

Handled in `ruleMatching.match_bucket()` per bucket.

### Process

1. Optional **in-bucket Connect** attempt (if policy allows)  
2. Sort riders by shortest time window  
3. Score all feasible pairs; pick non-overlapping pairs  
4. Expand pairs up to `MAX_GROUP_SIZE`  
5. Leftover passes: split 4→3+2, absorb leftovers, LAX/ONT shape fixes  

### Validity

A normal group must satisfy:

| Rule | Config |
|------|--------|
| Size 2–`MAX_GROUP_SIZE` | `MAX_GROUP_SIZE = 5` |
| Time overlap or touch | `ALLOW_TOUCHING`, `OVERLAP_GRACE_MIN` |
| Bag limits | `MAX_TOTAL_BAGS`, `MAX_LARGE_BAGS`, `LARGE_BAG_MULTIPLIER` |
| Terminals | `TERMINAL_MODE` — `"slack"` or `"strict"` |
| Same flight | `SAME_FLIGHT_PRIORITY` — prefer same airline/flight/date |

### Pickup time

Applied in `refresh_match_suggested_times()` (also after Connect merge):

| Direction | Rule |
|-----------|------|
| TO airport | 15 minutes **before** overlap end |
| FROM airport | 15 minutes **after** overlap start |

CSV uses these times; DB persist uses earliest-overlap fields for `Matches.date` / `time` (see [Schema](schema.md)).

---

## Connect shuttles

Large groups merged **after** normal matching. Controlled in `config.py`, interpreted by `connect_policy.py`, merged in `connect_merge.py`.

```python
CONNECT_DEPARTURE = ["LAX", "ONT"]   # school → airport; [] = off
CONNECT_ARRIVAL = ["LAX", "ONT"]     # airport → school; [] = off
CONNECT_SIZE1 = [6, 12]
CONNECT_SIZE2 = [12, 24]             # preferred when possible
```

| Rule | Detail |
|------|--------|
| Enable/disable | Both lists empty → Connect off entirely |
| Sizing | Overlap-based; bags ignored for Connect tier fit |
| Vouchers | **None** for Connect |
| DB | May absorb/delete smaller existing rides when merging |
| Type | `ride_type = "Connect"` |

### Post-match Connect steps (in `main.py`)

1. **`_final_lax_unmatched_retry`** — re-match Connect-scoped unmatched (if Connect enabled)  
2. **`merge_connect_with_existing`** — combine run groups + unmatched + **existing DB rides**  

---

## ONT post-processing

`_ont_post_process_unmatched()` after all buckets:

- Finds unmatched ONT riders and same-day groups of 4  
- When valid, splits 4+2 → 3+2 to place the unmatched rider  

---

## Subsidy

Applied in `main.apply_group_subsidy()` **after** final group list (including Connect merge).

```python
thresholds = {"LAX": 3, "ONT": 2}   # in main.py, not config.py
```

| Requirement | Detail |
|-------------|--------|
| Size | Group size ≥ threshold for airport |
| School | All riders Pomona |
| Date | In `COVERED_DATES_OUTBOUND` or `INBOUND` when `COVERED_DATES_EXPLICIT` is true |

Example covered dates in config (update per break):

```python
COVERED_DATES_EXPLICIT = True
COVERED_DATES_OUTBOUND = ["05-12", "05-13", ...]
COVERED_DATES_INBOUND = ["03-20", "03-21", "03-22"]
```

---

## Vouchers

`vouchers.assign_vouchers()` runs after subsidy.

| Rule | Detail |
|------|--------|
| Connect | No vouchers |
| Group voucher | Subsidized + covered groups only |
| Contingency | Inbound subsidized covered groups |
| Pool | Local CSV via `--vouchers` |
| Dry-run | Uses `.dryrun.csv` copy; does not consume real pool |

CSV columns: see [Schema → Voucher CSV](schema.md#voucher-csv).

---

## Uber vehicle sizing

For non-Connect groups, `main._determine_uber_type()` maps group size + bag units to `X`, `XL`, or `XXL`. Hard limits: 12 bag units for groups of 2–3; 10 for 4+.

---

## Where to change what

| Policy | Location |
|--------|----------|
| Bags, overlap, group size, terminal | `config.py` |
| Connect airports and tiers | `config.py` |
| Covered dates | `config.py` |
| Subsidy size thresholds | `main.py` |
| Scoring and pairing | `ruleMatching.py` |
| Connect merge with DB | `connect_merge.py` |
| Voucher rules | `vouchers.py` |

After changes: [Operations → dry-run](operations.md#first-run).

---

## Next steps

- [Glossary](glossary.md) — term definitions  
- [Schema](schema.md) — data written to Supabase/CSV  
- [Code guide](code-guide.md) — module responsibilities
