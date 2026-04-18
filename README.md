# profile-sync-validator

Local analysis script that validates traits and identifiers produced by the **Patch V3 archiver** against what the **warehouse-connectors loader** actually wrote to S3. It runs the validation queries from the runbook as a single Athena pipeline per (space, day, window).

## References

The SQL executed by this tool is ported from:

- [patch_v3_athena_final_queries.md](https://github.com/segmentio/archiver/blob/master/patch_v3_athena_final_queries.md) — in-repo copy in the archiver project.
- [Patch V3 Validation Runbook (Google Doc)](https://docs.google.com/document/d/1PiMEtj5PK_zpVuVQNZ2zzFpsRaJnLoI98dyylgD_2bs/edit) — authoritative source.

If either doc is updated, port the change into the templates under `validator/sql/`.

## What it does

For a single `(space_id, warehouse_id, day, time window)` and a warehouse target (Snowflake or BigQuery), the tool:

1. Drops + creates a `schema_reader_{artifact}_{space_id}` external table over the loader's `schema.json`.
2. Runs a meta-query to generate the `loader_{artifact}_{space_id}` external table DDL (column order varies per space), then executes it.
3. In parallel, runs the **inserts** and **deletes** validation queries, each wrapped as `SELECT COUNT(*) AS mismatch_count`.
4. Drops all tables it created on exit (success or failure).

The pipeline runs twice if both artifacts are requested — once for `identifiers`, once for `traits` — and those two pipelines run in parallel with each other.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | All validations passed (0 mismatches) |
| 1 | At least one validation query returned a non-zero count |
| 2 | System error (Athena call failed after 3 retries, bad input, AWS auth, etc.) |

## Usage

```bash
aws-okta exec prod-write -- go run . \
  --space-id spa_7dwJTm2tmQFEwcECEbYMu3 \
  --warehouse-id w5tevAWoyfNRmdisUcdavB \
  --schema-name personas_main_2 \
  --day 2026-04-17 \
  --start-at 2026-04-17T00:00:00Z \
  --end-at   2026-04-17T01:00:00Z \
  --tombstoned-start-at 2026-04-16T22:00:00Z \
  --tombstoned-end-at   2026-04-17T03:00:00Z \
  --warehouse snowflake \
  --athena-output s3://aws-athena-query-results-xxx/
```

### Flags

| Flag | Description |
|---|---|
| `--space-id` | Personas space ID, e.g. `spa_...` (required) |
| `--warehouse-id` | Warehouse source ID (required) |
| `--schema-name` | Project slug / schema name, e.g. `personas_main_2` (required) |
| `--day` | Day to validate, `YYYY-MM-DD` (required) |
| `--start-at` / `--end-at` | Main time window, RFC3339 (required) |
| `--tombstoned-start-at` / `--tombstoned-end-at` | Extended window for tombstone lookup — should be ≥2h wider on each side (required) |
| `--warehouse` | `snowflake` or `bigquery` (required) |
| `--athena-output` | S3 location for Athena query results (required) |
| `--artifacts` | `identifiers`, `traits`, or both comma-separated (default: both) |
| `--athena-db` | Athena database (default `patch_v3`) |
| `--athena-workgroup` | Athena workgroup (default `primary`) |
| `--data-v3-table` | Athena table with V3 patches (default `data_v3`) |

## Layout

```
main.go                       CLI entry point
validator/
  config.go                   ValidationRun, Artifact/Warehouse enums
  errors.go                   SystemError, ValidationError, MultiError
  athena.go                   Thin Athena SDK wrapper (retry + poll)
  artifact.go                 Identifiers vs Traits behavior
  target.go                   Snowflake vs BigQuery behavior
  templates.go                embed.FS + text/template renderer
  orchestrator.go             Per-artifact pipeline + cleanup
  sql/
    schema_reader_{artifact}_{warehouse}.sql.tmpl
    generate_ddl_{artifact}_{warehouse}.sql.tmpl
    inserts_{artifact}.sql.tmpl
    deletes_{artifact}.sql.tmpl
```

## Retries and errors

- Athena calls retry up to 3 times on system errors with `1s`, `4s`, `16s` backoff.
- Validation errors (non-zero mismatch count) are **not** retried — they represent real data discrepancies.
- If inserts succeed and deletes fail (or vice versa), both results are surfaced via `MultiError`.

## Known edge cases

See the Google Doc runbook for the full list. The most common sources of "expected" mismatches:

- **Tombstoned-in-window false positives** — if a segment is inserted/updated and then tombstoned inside the same sync window, the loader never writes the insert row. Widen `--tombstoned-start-at` / `--tombstoned-end-at` and use the investigation query from the runbook to confirm.
- **Trait key normalization** — the SQL approximates `go-snakecase` with regex. Unusual mixed-case patterns may produce spurious trait key mismatches.
