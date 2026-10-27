# Security review: Phase 2 (Spark ingestion into Databricks)

Date: 2026-10-27 · Reviewer: cybersecurity agent · Verdict: **Blocked until #1 is fixed**

| # | Sev | Finding | Fix | Owner | Status |
|---|---|---|---|---|---|
| 1 | Critical | A Databricks PAT is in `.env.example`, which is NOT git-ignored (meant to be committed) | Blank it, revoke that token, issue a new one into `.env` only, restart ingestor | user | value removed from `.env.example` (verified); revoke + rotate pending |
| 2 | High | Ingestor uses a personal access token (full user rights) | Service principal + OAuth M2M with only USE CATALOG/SCHEMA + READ/WRITE VOLUME on `landing` | devops (after user OK) | open |
| 3 | Medium | `account users` has USE_SCHEMA on `workspace.ecommerce` and SELECT/INSERT/UPDATE on `bronze_orders` | Revoke INSERT/UPDATE (keep SELECT if needed) | devops (after user OK) | open |
| 4 | Medium | Agent deny rules miss `.databricks/.databricks.env`, `~/.databrickscfg`, and Bash `cat` | Add Read + Bash deny rules | user (permission config) | open |
| 5 | Medium | Local dev deps (urllib3, starlette) have advisories; container is clean | Re-run `databricks environments setup-local` when constraints update | devops | open |
| 6 | Low | Dockerfile: pip deps of databricks-sdk not hash-pinned | constraints file with `--require-hashes` | devops | open |
| 7 | Low | Base image pinned by tag, not digest | pin `@sha256` | devops | open |
| 8 | Low | spark-ingestor lacks `no-new-privileges`, `cap_drop: [ALL]` | add to compose | devops | open |
| 9 | Low | bronze notebook doesn't validate catalog/schema widgets before SQL | identifier regex check | data-engineer | open |
| 10 | Low | Workspace host committed (not a secret) | env var if repo goes public | devops | open |
| 11 | Low | `pyproject.toml.bak`, duplicate `.databricks` in `.gitignore` | cleanup | devops | open |

Verified clean: no token in image/`docker history`/Spark UI/logs; git history clean; job paused; `.env` files ignored; both PATs expire in 14 days.
Phase 1 #6 (Kafka SASL/TLS) is moot — Kafka stays local.
