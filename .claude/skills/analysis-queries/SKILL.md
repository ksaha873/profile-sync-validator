---
name: analysis-queries
description: Generate row-level Athena diagnostic queries from the counting templates in internal/validator/sql/. Use when the validator reports a non-zero mismatch/reverse_mismatch and the user wants to inspect which rows are unmatched (e.g. "show me the 108 unmatched traits inserts", "debug the reverse mismatch", "why is this segment id failing"). Scoped to the profile-sync-validator repo.
---

# Generating analysis queries for validator mismatches

The validator (`cmd/profile-sync-validator`) renders SQL templates from `internal/validator/sql/*.tmpl` and wraps each final query in `SELECT COUNT(*) …` style aggregates, so the output is counts, not rows. When counts are non-zero, the next step is always: produce a variant of the same query that returns the offending rows with enough context to diagnose.

This skill documents the mechanical recipe.

## Guardrails

- This skill produces **analysis query text printed to the terminal**, for the user to paste into Athena. Do not execute the queries.
- Do not edit files, templates, or committed SQL without explicit user permission. This skill is for *generating* diagnostic queries, not for modifying the repo.
- If the investigation reveals a filter bug or template issue, stop and propose the fix to the user. Do not open templates or modify `patch_v3_athena_final_queries.md` until they say to proceed.
- Do not run `git add`, `git commit`, or `git push` as part of analysis work.
- Do not publish to the Google Doc or any shared system without explicit user approval.
- Do not invoke the validator binary (`go run ./cmd/profile-sync-validator`), the AWS Athena SDK, `aws-okta`, or any network tool to run these queries. Emit the SQL as text; the user runs it.
- If a scratch file is genuinely needed for formatting, ask first and write it under `/tmp/`, never inside the repo.

## Prerequisites

Before an analysis query will run, the external tables the templates reference must exist in Athena. The validator drops them on exit, so you'll usually need to recreate them first. Render the setup pair from:
- `internal/validator/sql/schema_reader_{artifact}_{warehouse}.sql.tmpl`
- `internal/validator/sql/generate_ddl_{artifact}_{warehouse}.sql.tmpl`

substituting the same params the validator used.

The required substitutions are defined in `internal/validator/artifact.go` (`validationTmplData`) and `internal/validator/target.go` (`schemaReaderTmplData`, `generateDDLTmplData`). The `LoaderTable` is `loader_{artifact}_{SpaceID}` and `SchemaReaderTable` is `schema_reader_{artifact}_{SpaceID}`.

## Recipe: turn a template into an analysis query

For any of the eight validation templates (`inserts_{identifiers,traits}`, `deletes_{identifiers,traits}`, `tombstoned_check_deletes_{identifiers,traits}`), the transformation is:

1. **Render the template** with concrete values (do the substitution manually; there's no CLI for this).
2. **Replace the final aggregate `SELECT`** with a `SELECT` that projects the per-row columns of interest.
3. **Enrich the intermediate CTEs** with debug columns the aggregate discarded (e.g. `raw_key`, `trait_status`, `prev_value`, `curr_value`, `s3_path`, the raw `__profile_version`, `__deleted`).
4. **Add a `LIMIT`** (usually 1000) — the mismatch set can be large.

### Forward mismatch (patches side has rows the loader doesn't)

The final `SELECT` in the counting query is always:
```sql
(SELECT COUNT(*) FROM <patches_cte> p LEFT JOIN <loader_cte> l
   ON p.id = l.id AND p.accepted_at = l.accepted_at
 WHERE l.id IS NULL) AS mismatch_count
```

Replace with:
```sql
SELECT p.canonical_segment_id, p.id, p.raw_key, p.accepted_at, p.s3_path
FROM <patches_cte> p
  LEFT JOIN <loader_cte> l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL
ORDER BY p.accepted_at
LIMIT 1000;
```

**Tip:** also LEFT JOIN against the loader on `id` alone to distinguish "id missing entirely" from "id exists with a different accepted_at" (timestamp skew):
```sql
  LEFT JOIN <loader_cte> l_any ON p.id = l_any.id
...
SELECT ..., l_any.accepted_at AS loader_accepted_at_any_row
```

### Reverse mismatch (loader side has rows patches don't — deletes only)

The final `SELECT` for deletes is:
```sql
(SELECT COUNT(*) FROM <loader_cte> l LEFT JOIN <expected_cte> p
   ON p.id = l.id AND p.accepted_at = l.accepted_at
 WHERE p.id IS NULL) AS reverse_mismatch_count
```

Replace with:
```sql
SELECT l.canonical_segment_id, l.id, l.name AS loader_trait_name,
       l.accepted_at AS loader_accepted_at,
       l.profile_version_raw, l.deleted_raw, l.s3_path AS loader_s3_path
FROM <loader_cte> l
  LEFT JOIN <expected_cte> p ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE p.id IS NULL
ORDER BY l.accepted_at
LIMIT 1000;
```

For this to produce useful columns, enrich the `loader` CTE to carry the raw source columns:
```sql
loader AS (
    SELECT l.canonical_segment_id, lower(l.id) AS id, l.name,
        l.__profile_version AS profile_version_raw,
        l.__deleted AS deleted_raw,
        l."$path" AS s3_path,
        date_format(...) AS accepted_at
    FROM <loader_table> l
    WHERE ...
)
```

### Tombstone-check variant

`tombstoned_check_deletes_{artifact}.sql.tmpl` ends with `SELECT COUNT(*) FROM unmatched WHERE affected_segment_id NOT IN (...)`. For diagnosis, replace the final `SELECT` with the full `unmatched` row projection plus a flag indicating whether the row was tombstone-excluded:

```sql
SELECT u.*,
       (u.affected_segment_id IN (SELECT segment_id FROM tombstoned_segments)) AS was_tombstoned
FROM unmatched u
ORDER BY u.accepted_at
LIMIT 1000;
```

## Columns worth surfacing for traits diagnosis

When debugging trait inserts/deletes, carry these from `normalize_key` all the way through to the final projection:

| column | why |
|---|---|
| `raw_key` | the literal key the loader saw (before snakecase) |
| `trait_key` | the snakecased form used in the id hash |
| `trait_status` (from `tr.status`) | UNCHANGED / CHANGED / CONFLICT / UNKNOWN |
| `prev_value`, `curr_value` | verify loader emission rules (see `downloader/profiles/trait.go` in warehouse-connectors) |
| `summary` | NORMAL / CREATED / REEMIT / MERGED — MERGED bypasses most filters |
| `s3_path` | locate the raw patch JSON in `segment-profiles` |

For identifiers, surface `id.type`, `id.id`, `id.status`, and for links, `lnk.group_id`, `lnk.status`.

## Loader-side diagnostic columns

For any loader CTE (Snowflake or BigQuery), these raw columns are usually load-bearing:

- `__profile_version` (raw bigint nanoseconds — the source of the `accepted_at` comparison)
- `__deleted` (`'true'` / `'false'` / `''` / SQL NULL)
- `"$path"` (S3 object the row came from)
- `canonical_segment_id`, `id`, `name`, `value` (materialized view columns)

Casting and date formatting reuse the same `from_unixtime(... / 1e9)` pattern as the template.

## Example: the trait inserts forward-mismatch query

Given a validator failure like:
```
[traits inserts] expected=N loader=M mismatch=108
```

A good diagnostic variant:
1. Start from the rendered `inserts_traits.sql.tmpl`.
2. Enrich `normalize_key` with `summary` and `raw_key` columns.
3. Carry those through to `patches`.
4. Replace the trailing three-count `SELECT` with the forward-mismatch projection shown above, joined both on `(id, accepted_at)` and `(id)` so you can distinguish missing vs. skewed.

## When NOT to use this skill

- When the validator exits 0 — no diagnosis needed.
- When the request is to change the *validation semantics* (i.e. fix a filter bug). In that case stop and propose the fix to the user; do not edit `internal/validator/sql/` or `patch_v3_athena_final_queries.md` without permission.
- When the request is to diagnose `__profile_version` timestamp skew specifically — that needs the `id`-only cross-join above, plus the trait's full history in `data_v3`:
  ```sql
  SELECT * FROM data_v3
  WHERE namespace_id = '{space_id}'
    AND day_written = CAST(CAST(to_unixtime(date_parse('{day}','%Y-%m-%d'))*1000 AS BIGINT) AS VARCHAR)
    AND segment_id = '{segment_id}'
  ORDER BY accepted_at;
  ```
