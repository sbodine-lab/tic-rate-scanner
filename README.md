# TiC rate scanner

Streams health plans' Transparency in Coverage in-network files and extracts
negotiated rates for a configured set of billing codes and organizations,
with the source file and its `last_updated_on` kept on every rate.

This repository holds code only. The payer list, organization identifiers and
roster are committed solely as `config/config.tar.age`, and every result is
encrypted to `scanner.pub` before it leaves the runner. Run logs show counts
and statuses, never names, identifiers or amounts.

- `.github/workflows/scan.yml` runs on the 13th of each month (retry on the
  17th) and on demand: plan, 16 parallel shards, combine. A run fails when any
  payer is missing an expected file, is older than its allowed age, or has a
  file that could not be read completely.
- Each run on `main` attaches its encrypted output to a `scan-YYYY-MM-DD-<run>`
  release. A run started from another branch (to try a change on a few payers)
  keeps it as a 3-day `scan-result` artifact instead and never pushes to `main`.
- `last-scan.json` records the latest run.

Tests use synthetic data only: `python -m unittest discover -s tests`.
