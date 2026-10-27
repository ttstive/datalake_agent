# Security review: Phase 1 (local streaming)

Date: 2026-10-27 · Reviewer: cybersecurity agent · Verdict: **OK for local use**

No secrets in git history or tracked files; `.env` files are git-ignored. Runtime deps (confluent-kafka, python-dotenv) have no known vulnerabilities.

## Must fix before Phase 2
| # | Finding | Owner |
|---|---|---|
| 2 | Agent deny rules miss `.databricks/.databricks.env` and don't block `cat` via Bash | user decision (permission config) |
| 6 | Producer: accept only `PLAINTEXT` or `SASL_SSL`; set TLS endpoint verification | data-engineer |
| 8 | Prefer OAuth / service principal over personal access tokens; Kafka creds in a secret scope | devops-engineer |
| 1 | Dev deps from Databricks constraints (urllib3, requests, certifi, pyasn1, pyarrow, pytest) have advisories; re-audit, bump when constraints allow | devops-engineer |

## Low / hygiene
- ~~Kafka UI image unmaintained~~ → switched to `ghcr.io/kafbat/kafka-ui:v1.4.2` (localhost only, no auth; now starts with Kafka by default).
- Pin Docker images by digest (optional).
- `fivetran_project/profiles.yml` commits workspace host + warehouse path (not secrets); move to env vars if the repo is public.
- Remove unused `pip` runtime dep, duplicate `.databricks` line in `.gitignore`, stray `pyproject.toml.bak`.
- `customer_id` is a pseudo-identifier; document as such in silver/gold.
