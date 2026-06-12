# Glossary

Quick definitions for terms used across this repo and docs.

| Term | Definition |
|------|------------|
| **Flight form** | A student’s airport ride signup; one row in Supabase `Flights`. |
| **Unmatched flight** | `Flights.matching_status` is `submitted`, `unmatched`, null, or another non-`matched` value. Only flights that are not already `matched` are loaded for matching. |
| **RiderLite** | In-memory rider object (`rider_data.py`) combining flight + user fields. |
| **Bucket** | Subset of riders with same direction, airport, and school compatibility. Matching never crosses buckets. |
| **Bucket key** | String label, e.g. `TO LAX \| POMONA`. |
| **TO** | School → airport trip (`to_airport = true`). |
| **FROM** | Airport → school trip (`to_airport = false`). |
| **Match** | One ride group (`ruleMatching.Match`): 2–5 normal riders or a Connect shuttle. |
| **Normal group** | Standard Uber-sized match (2–5 riders) with bag and terminal rules. |
| **Connect** | Large shared shuttle (typically 6–24 riders); overlap-based sizing; no vouchers. |
| **Overlap window** | Time range where all riders’ `earliest_time`–`latest_time` intervals overlap (or touch, if allowed). |
| **Suggested time** | Pickup time after 15-minute before/after overlap rules. |
| **Subsidy** | Pomona group ride partially funded; `group_subsidy` / `is_subsidized` when rules pass. |
| **Covered date** | MM-DD in config lists; subsidy/vouchers only on these dates when explicit mode is on. |
| **Group voucher** | Uber credit link assigned to a subsidized covered group. |
| **Contingency voucher** | Per-rider backup voucher (inbound subsidized covered groups). |
| **Dry-run** | Full pipeline logic with live reads but no DB match writes and no real voucher consumption. |
| **Production run** | Writes `Rides`/`Matches`, updates `Flights`, consumes vouchers. |
| **AlgorithmStatus** | Supabase table tracking matcher run state (production only). |
| **Connect merge** | Post-step combining current run + unmatched + existing DB rides into Connect tiers. |
| **original_unmatched** | Flight flag: `true` if still unmatched after a production run. |

See also: [Platform overview](platform-overview.md) · [Schema](schema.md)
