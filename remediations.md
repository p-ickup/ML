# ML Remediations

This document is the written remediation record for the ML matching branch.
It is organized by remediation issue number and is intended to support ASPC
review of remediation work requested to be completed no later than June 19,
2026.

## ASPC Written Notice Summary

Remediation work has been completed for the production atomicity, Supabase
side-effect integration test, and cross-midnight output-window issues. The main
change is that ML production writes now go through one transactional
Supabase RPC instead of separate client-side ride, match, flight, voucher, and
Connect cleanup writes. Voucher state now lives in `public."Vouchers"`, commit
runs are tracked for idempotency, and live DB integration tests verify the core
commit behavior.

Supporting repository updates include new SQL migration/support files,
production payload construction, voucher CSV import tooling, documentation
updates, and live Supabase integration tests that touch the configured database
and clean up their test rows.

## Remediation Issue #1

**Audit item:**

Algorithm/main.py (line 408) inserts Rides, then inserts Matches, then updates Flights;
Algorithm/connect_merge.py (line 420) may delete existing matches/rides before new
DB writes; Algorithm/vouchers.py (line 172) mutates the voucher CSV before production
DB writes. There is no transaction, rollback, or retry strategy. A mid-run failure could
consume vouchers, delete/replace rides, or leave flights marked inconsistently.
Remediation: Implement transaction handling, rollback procedures, retry mechanisms,
or equivalent safeguards sufficient to prevent inconsistent records, orphaned records,
voucher consumption errors, or corrupted operational state.

**Status:** Completed

**Summary:**

The production path now makes one validated commit request to a transactional Supabase RPC instead of writing rides, matches, flights, vouchers, and Connect cleanup changes one at a time. That RPC is defined in [documentation/sql/001_commit_matching_run.sql](/Users/xmora/Documents/Pickup/ML/documentation/sql/001_commit_matching_run.sql), records each run in `public."MatchingRuns"`, consumes vouchers from `public."Vouchers"` inside the same transaction, and rolls back the whole commit if any required write fails. `Algorithm/connect_merge.py` now only reports cleanup intent, and `Algorithm/vouchers.py` is limited to dry-run CSV voucher assignment. Additional constraints and indexes in [documentation/sql/002_integrity_safeguards.sql](/Users/xmora/Documents/Pickup/ML/documentation/sql/002_integrity_safeguards.sql) protect against orphaned matches, duplicate match rows, and inconsistent voucher state.

**Remediation completed:**

- Added `public."Vouchers"` as the production source of truth for voucher availability and consumption, with admin-only RLS for normal authenticated access.
- Added `public."MatchingRuns"` and `public.commit_matching_run(p_run_id uuid, p_payload jsonb)` in [documentation/sql/001_commit_matching_run.sql](/Users/xmora/Documents/Pickup/ML/documentation/sql/001_commit_matching_run.sql).
- Replaced the old production write sequence in `Algorithm/main.py` with one validated RPC commit and transient retry using the same `run_id`.
- Removed direct client-side Connect cleanup writes from `Algorithm/connect_merge.py`; cleanup is now part of the RPC transaction.
- Kept voucher CSV mutation out of production DB writes; CSV voucher assignment remains dry-run only.
- Added integrity safeguards in [documentation/sql/002_integrity_safeguards.sql](/Users/xmora/Documents/Pickup/ML/documentation/sql/002_integrity_safeguards.sql) for match foreign keys, duplicate match prevention, and voucher usage consistency.
- Added voucher CSV import tooling and live Supabase integration tests for the DB side effects.

**Safeguards implemented:**

- **Transaction and rollback:** Connect cleanup, `Rides`, `Matches`, `Flights`, voucher consumption, and `MatchingRuns` updates are committed together or rolled back together by Postgres.
- **Retry and idempotency:** transient RPC failures are retried with the same `run_id`; `MatchingRuns.run_id` and `payload_hash` prevent duplicate or conflicting commits.
- **Voucher protection:** vouchers are selected, locked, marked used, and linked to the committed ride/flight inside the same transaction.
- **Integrity protection:** database constraints/indexes guard against orphaned matches, duplicate flight matches, and inconsistent voucher usage state.

**Policies and permissions:**

- `public."Vouchers"` has row level security enabled.
- Only users with `public."Users".role in ('admin', 'super_admin')` can select, insert, update, or delete voucher rows through normal authenticated access.
- Anonymous users and ordinary authenticated users do not have direct voucher table access.
- `public."MatchingRuns"` has row level security enabled.
- Direct table access to `public."MatchingRuns"` is revoked from `anon` and `authenticated`.
- `public.commit_matching_run(uuid, jsonb)` execution is revoked from `public`, `anon`, and `authenticated`.
- `public.commit_matching_run(uuid, jsonb)` execution is granted to `service_role`.

**Created files:**

- `Algorithm/commit_payload.py` - builds and validates production commit payloads and wraps the RPC call with retry behavior.
- `Algorithm/import_vouchers.py` - validates voucher CSVs and inserts missing voucher links into `public."Vouchers"` without overwriting existing voucher state.
- [documentation/sql/001_commit_matching_run.sql](/Users/xmora/Documents/Pickup/ML/documentation/sql/001_commit_matching_run.sql) - creates `MatchingRuns` and the transactional commit RPC.
- [documentation/sql/002_integrity_safeguards.sql](/Users/xmora/Documents/Pickup/ML/documentation/sql/002_integrity_safeguards.sql) - contains diagnostics plus integrity constraints/indexes.
- `tests/test_commit_payload.py` - unit coverage for payload validation and retry behavior.
- `tests/test_import_vouchers.py` - unit coverage for voucher CSV validation and import mapping.
- `tests/integration_supabase.py` - live Supabase integration test entrypoint.
- `tests/integration_supabase_base.py` - shared live Supabase test setup and cleanup helpers.
- `tests/integration_supabase_commit.py` - live DB coverage for voucher import and direct commit RPC side effects.
- `tests/integration_supabase_pipeline.py` - live DB coverage for `AlgorithmStatus` and `main.run(...)` lifecycle side effects.

**Updated files:**

- `Algorithm/main.py`
- `Algorithm/connect_merge.py`
- `Algorithm/vouchers.py`
- `README.md`
- `documentation/operations.md`
- `documentation/platform-overview.md`
- `documentation/pipeline_diagram.html`
- `documentation/schema.md`
- `documentation/code-guide.md`
- `documentation/matching-rules.md`

**How to run relevant remediation pieces:**

Run unit tests:

```bash
python3 -m unittest discover -s tests -t .
```

Run live Supabase integration tests:

```bash
python3 -m unittest tests.integration_supabase
```

Validate voucher CSV import without writing to Supabase:

```bash
python3 Algorithm/import_vouchers.py vouchers/SpringBreak.csv
```

Import a voucher CSV into `public."Vouchers"`:

```bash
python3 Algorithm/import_vouchers.py vouchers/SpringBreak.csv --commit
```

This import command is safe to run before ML production execution. It inserts
missing voucher links only and leaves existing rows untouched.

Optional syntax check:

```bash
python3 -m py_compile Algorithm/*.py tests/integration_supabase*.py
```

**Verification completed:**

- `python3 -m unittest discover -s tests -t .`
  - Result: passed
  - Tests run: 90
  - Skipped: 0
- `python3 -m unittest tests.integration_supabase`
  - Result: passed
  - Tests in current suite: 19
  - Verified live DB voucher import, transactional commit success, `Rides` inserts, `Matches` inserts, `Flights` matched/unmatched updates, voucher consumption/audit fields, `MatchingRuns` ledger updates, idempotent replay, rollback on mid-commit DB failure, Connect cleanup, direct `AlgorithmStatus` behavior, `main.run(...)` lifecycle success/failure scenarios, and persisted cross-midnight `Matches` windows.
- `python3 Algorithm/import_vouchers.py vouchers/SpringBreak.csv`
  - Result: passed
  - Validated rows: 4,400
- `python3 -m py_compile Algorithm/*.py tests/integration_supabase*.py`
  - Result: passed

**Notes for ASPC review:**

- The live Supabase integration tests intentionally touch the configured Supabase database. Generated matching artifacts are removed; the reusable ten-flight scenario parks its dedicated forms for later runs.
- [documentation/sql/002_integrity_safeguards.sql](/Users/xmora/Documents/Pickup/ML/documentation/sql/002_integrity_safeguards.sql) includes diagnostic SELECTs that should be run before applying the constraint/index section. The constraint/index section should only be applied after diagnostics return no rows.

## Remediation Issue #2

**Audit item:**

No integration test coverage for Supabase side effects.
The tests cover only matching/voucher logic, but not production DB writes, Connect
cleanup, AlgorithmStatus, or rollback behavior
Remediation: Implement integration tests covering production database writes, Connect
cleanup operations, AlgorithmStatus behavior, and rollback scenarios.

**Status:** Completed

**Summary:**

Live Supabase integration coverage has been added for the exact side effects named in the remediation: production database writes, Connect cleanup operations, `AlgorithmStatus` behavior, and rollback scenarios. The suite performs real Supabase operations with controlled inputs and verifies successful production paths plus failures before and during the transactional commit.

**Remediation completed:**

- `tests/integration_supabase.py` remains the single command entrypoint for the live DB suite.
- `tests/integration_supabase_base.py` provides the shared Supabase client, temporary row fixtures, and cleanup helpers.
- `tests/integration_supabase_commit.py` covers voucher import and direct `commit_matching_run` RPC side effects.
- `tests/integration_supabase_pipeline.py` covers direct `AlgorithmStatus` behavior and `main.run(...)` lifecycle side effects.
- The integration suite removes generated database artifacts; one reusable scenario retains ten dedicated flight fixtures parked on a past date.
- The suite verifies the tracked, non-production `tests/fixtures/TestVouchers.csv` fixture imports into `public."Vouchers"`.
- Because the voucher fixture is tracked outside the ignored operational `vouchers/` directory, the integration suite has the same input after a clean checkout.
- The suite verifies `commit_matching_run` inserts `Rides`, inserts `Matches`, updates matched and unmatched `Flights`, consumes vouchers, writes `MatchingRuns`, supports idempotent replay, rolls back on mid-commit failure, and performs Connect cleanup.
- The suite verifies direct `AlgorithmStatus` behavior: creating a running row, reusing a due scheduled row, marking success, and marking failure with an error message.
- The suite verifies `main.run(...)` production behavior for a normal match run, no candidate riders, one rider with no match, Connect enabled without cleanup, Connect merge replacing an existing match/ride, failure before commit, failure during commit, and persisted `Matches` windows for broad, tight, and three-person cross-midnight groups.
- A ten-flight production-path test verifies seven overlapping LAX forms become one Connect ride, two ONT forms become a separate ride, and one non-overlapping form remains unmatched. It deletes generated matches/rides and reuses the parked forms on later runs.
- Failure tests verify `AlgorithmStatus.failed`, `error_message`, absence of unexpected production side effects, and RPC rollback of partial ride/match writes.
- Unit coverage verifies commit payload validation, retry behavior, voucher CSV parsing/import mapping, matching logic, Connect policy, bucket behavior, and dry-run voucher assignment.
- Connect regression coverage verifies normal matches survive when no Connect group forms and existing DB rides are not resubmitted as new matches.

**AlgorithmStatus table details used for testing:**

- Required columns: `algorithm_name`, `scheduled_for`, `status`.
- Nullable lifecycle columns: `started_at`, `finished_at`, `run_id`, `error_message`, `target`.
- `id` defaults to `gen_random_uuid()`.
- `created_at` defaults to `now()`.
- Current RLS policy allows authenticated admin SELECT through `algorithm_status_select_admin`; integration tests use the configured service-role client.

**How to run:**

```bash
python3 -m unittest tests.integration_supabase
```

**Verification completed:**

- Current live DB suite: 19 tests. The original 18-test suite passed, and the new reusable ten-flight scenario passed on both initial creation and fixture reuse.
- Current DB coverage: voucher import, direct `AlgorithmStatus` behavior, `main.run(...)` lifecycle success/no-rider/no-match/Connect/failure scenarios, transactional commit success, idempotent replay, rollback, Connect cleanup, persisted cross-midnight windows, and the ten-flight Connect/ONT/unmatched scenario.
- `python3 -m unittest discover -s tests -t .`
  - Result: passed
  - Tests run: 90

**Supporting documentation updated:**

- `README.md` now lists the live Supabase integration test command.
- `documentation/operations.md` separates unit test commands from DB integration test commands.
- `documentation/code-guide.md` references the explicit live Supabase integration command and split test layout.

## Remediation Issue #3

**Audit item:** Cross-midnight output windows can be wrong.

**Status:** Completed

**Summary:** Cross-midnight group windows now use the same rule for matching, dry-run CSV output, and production DB writes: if `latest_time` is earlier than `earliest_time`, the end time is treated as the next day. This keeps persisted group windows consistent with the matcher.

**Remediation completed:**
- Added a shared normalized group-window helper used by matching output and production payload construction.
- Verified dry-run CSV output and production `Matches` rows persist the normalized cross-midnight overlap.
- Added live Supabase coverage for three production-path cases: a broad two-rider overnight window, a tight two-rider overnight window, and a mixed three-rider overnight group.

**Supporting documentation:** This section documents the canonical cross-midnight rule. The integration tests verify the rule in real `Matches` rows written through `main.run(...)`.

**Test results:**
- `python3 -m unittest tests.test_time_windows tests.test_rule_matching tests.test_commit_payload tests.test_main_csv` - 34 targeted cross-midnight unit tests passed.
- `python3 -m unittest tests.integration_supabase` - current suite contains 19 live tests.
- `python3 -m unittest discover -s tests -t .` - 90 tests passed.

**Repository updates:**
- `Algorithm/time_windows.py`
- `Algorithm/ruleMatching.py`
- `Algorithm/commit_payload.py`
- `tests/test_time_windows.py`
- `tests/test_rule_matching.py`
- `tests/test_commit_payload.py`
- `tests/test_main_csv.py`
- `tests/integration_supabase_pipeline.py`

## Remediation Issue #5

**Audit item:** Matching Lifecycle Has Gaps

**Status:** Completed for ML-owned matching lifecycle

**Summary:** The ML repo now uses the intended three-state matching lifecycle: `submitted`, `matched`, and `unmatched`. New or unknown values normalize to `submitted`, ML skips only `matched` flights, and production commits explicitly write `matched` or `unmatched`.

**Remediation completed:**
- Replaced ML reads of `Flights.matched` with `Flights.matching_status`.
- Explicitly recognizes `submitted`, `matched`, and `unmatched`; missing or unknown values safely normalize to `submitted`.
- Updated `RiderLite` and Connect merge rebuilding to carry `matching_status`.
- Updated the commit RPC so matched flights are written as `matching_status = 'matched'` and still-unmatched flights as `matching_status = 'unmatched'`.
- Updated integration tests and docs to verify the new status field.

**Supporting documentation:** Schema, operations, glossary, code guide, and SQL RPC documentation now reference `matching_status`.

**Note:** LAX and ONT subsidy minimum group sizes are configurable through `SUBSIDY_MIN_GROUP_SIZE` in `Algorithm/config.py`.

**Test results:**
- `python3 -m unittest discover -s tests -t .` - 90 local tests passed.
- `python3 -m ruff check .` - passed.
- `python3 -m unittest tests.integration_supabase` - 19 live Supabase tests passed.

**Repository updates:**
- `Algorithm/rider_data.py`
- `Algorithm/connect_merge.py`
- `documentation/sql/001_commit_matching_run.sql`
- `tests/integration_supabase_base.py`
- `tests/integration_supabase_commit.py`
- `tests/integration_supabase_pipeline.py`
- `tests/fixtures/TestVouchers.csv`

## Remediation Issue #11

**Audit item:** CI Does Not Provide Strong Release Assurance

**Status:** Completed for ML-equivalent scope

**Summary:** Although issue #11 was in reference to frontend release assurance, the ML repo now has CI gates appropriate for this Python batch pipeline. Pull requests and pushes to `main` or `remediation` install pinned dependencies, verify dependency compatibility and known vulnerabilities, compile the Python sources, check the SQL RPC contract, run Ruff, and run the unit suite. Frontend builds, API authorization, and deadline enforcement are outside this repository's scope.

**Remediation completed:**
- Added a GitHub Actions workflow for the ML repo.
- CI installs pinned, tested direct dependencies from `requirements.txt`.
- CI runs `pip check` and `pip-audit` so incompatible or known-vulnerable dependency sets fail the workflow.
- CI compiles production and test modules before linting.
- Added a local schema/RPC contract test for the transactional function signature, service-role permission, write set, and matching-status updates.
- CI runs `python -m ruff check .`.
- CI runs `python -m unittest discover -s tests -t .`.

**Supporting documentation:** The workflow and operations guide document the enforced ML checks. The live Supabase suite remains an explicit credentialed test because pull-request CI does not receive production service-role credentials.

**Test results:**
- `python3 -m ruff check .` - passed locally.
- `python3 -m unittest discover -s tests -t .` - 90 local tests passed.
- Clean-environment dependency installation and `pip check` passed.
- `pip-audit` reported no known vulnerabilities in the pinned requirements.
- Python source compilation and the SQL RPC contract checks passed.
- The reusable ten-flight live Supabase test passed with the pinned dependencies.

**Repository updates:**
- `.github/workflows/ci.yml`
- `tests/test_sql_contract.py`
- `requirements.txt`

## Remediation Issue #12

**Audit item:** Unused or stale components

**Status:** Completed

**Summary:** The ML repo was cleaned up so production code and tracked files better match the batch pipeline that is actually maintained. Stale local artifacts and the obsolete web-API Docker entrypoint were removed, dead/commented matching code was trimmed, and Ruff was added as the Python lint gate for unused imports, unused locals, unused production arguments, and syntax-level issues. The TypeScript/provider portions of the audit item do not apply to this Python-only ML repo.

**Remediation completed:**
- Removed tracked local/cache artifacts, including `.DS_Store`, Python bytecode caches, and editor workspace settings.
- Removed the stale Dockerfile for the old `MLapi` web entrypoint; the current ML repo runs as a Python batch pipeline.
- Removed stale commented/debug code and unused helper/local code from active matching and audit modules.
- Added Ruff configuration and documented `python3 -m ruff check .` as the repo lint command.

**Supporting documentation:** README, operations, and code guide now document the current batch pipeline and Ruff lint command.

**Test results:**
- Dependency import verification passed for pinned `pandas`, `python-dotenv`, `supabase`, and `ruff`.
- `python3 -m unittest tests.test_rule_matching tests.test_audit tests.test_main_csv` - 30 targeted tests passed.
- `python3 -m unittest discover -s tests -t .` - 90 local tests passed.
- `python3 -m ruff check .` - passed.

**Repository updates:**
- `.gitignore`
- `Dockerfile` removed
- `.vscode/settings.json` removed from version control
- `pyproject.toml`
- `requirements.txt`
- `Algorithm/main.py`
- `Algorithm/audit.py`
- `Algorithm/ruleMatching.py`
- `README.md`
- `documentation/operations.md`
- `documentation/code-guide.md`
