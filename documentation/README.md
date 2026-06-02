# Pickup ML documentation

Onboarding and reference for this repository. Read in order the first time; use individual pages when you need a specific topic.

## Recommended reading order

| # | Doc | Read when you need to… |
|---|-----|------------------------|
| 1 | [Platform overview](platform-overview.md) | Understand what Pickup matching is and how this repo fits in |
| 2 | [Repository & code guide](code-guide.md) | Know what each folder and Python file does |
| 3 | [Schema & data](schema.md) | Understand Supabase tables, CSV outputs, and in-memory types |
| 4 | [Operations](operations.md) | Install, run dry-run/production, verify results, troubleshoot |
| 5 | [Matching rules](matching-rules.md) | Change bucketing, Connect, subsidy, or voucher behavior |
| 6 | [Glossary](glossary.md) | Look up a term (bucket, Connect, RiderLite, …) |

## Visual & reference

| Resource | Purpose |
|----------|---------|
| [pipeline_diagram.html](pipeline_diagram.html) | Interactive pipeline flow (pull / push / next step) |
| [../SBOM.spdx](../SBOM.spdx) | Dependency inventory + licenses (SPDX, FOSSA-generated) |
| [../README.md](../README.md) | Repo entry point — quick start and links here |

## Quick jump

| I want to… | Go to |
|------------|-------|
| Run my first dry-run | [Operations → First run](operations.md#first-run) |
| See Supabase tables the pipeline touches | [Schema → Supabase](schema.md#supabase-tables) |
| Change Connect airports or sizes | [Matching rules → Connect](matching-rules.md#connect-shuttles) |
| Debug unmatched riders | [Operations → Troubleshooting](operations.md#troubleshooting) |
| Find which file to edit | [Code guide → I want to…](code-guide.md#i-want-to) |
