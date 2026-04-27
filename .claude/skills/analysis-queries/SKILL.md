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

Before an analysis query will run, the external tables the templates reference must exist in Athena. The validator drops them on exit, so you'll usually need to recreate them first.

The templates live at:
- `internal/validator/sql/schema_reader_{artifact}_{warehouse}.sql.tmpl`
- `internal/validator/sql/generate_ddl_{artifact}_{warehouse}.sql.tmpl`

Substitutions are defined in `internal/validator/artifact.go` (`validationTmplData`) and `internal/validator/target.go` (`schemaReaderTmplData`, `generateDDLTmplData`). The `LoaderTable` is `loader_{artifact}_{SpaceID}` and `SchemaReaderTable` is `schema_reader_{artifact}_{SpaceID}`.

### Prereq DDL — copy-paste recipe

Always emit these in the order shown: schema reader, then meta-query (its output is a CREATE EXTERNAL TABLE string that must be run as a separate query).

Buckets & path patterns by warehouse:

| warehouse | bucket | schema reader column shape | output path LIKE |
|---|---|---|---|
| snowflake | `segment-warehouse-snowflake` | `columns array<struct<name:string, type:string>>` | `%output%csv.gz` |
| bigquery | `segment-warehouse-bigquery`  | `bigquery array<struct<Name:string, Type:string>>` | `%output%.json.gz` |

Path prefix in both cases: `s3://{bucket}/{warehouse_id}/personas_{space_id}/{schema_name}/user_{artifact}/`.

#### Snowflake — traits (swap `traits`→`identifiers` and `user_traits/`→`user_identifiers/` for the identifiers variant)

**Step 1 — schema reader:**
```sql
DROP TABLE IF EXISTS schema_reader_traits_{space_id};

CREATE EXTERNAL TABLE schema_reader_traits_{space_id} (
    columns array<struct<name:string, type:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://segment-warehouse-snowflake/{warehouse_id}/personas_{space_id}/{schema_name}/user_traits/';
```

**Step 2 — meta-query (output is a DDL string; run that string next):**
```sql
DROP TABLE IF EXISTS loader_traits_{space_id};

SELECT 'CREATE EXTERNAL TABLE loader_traits_{space_id} (' || chr(10)
    || array_join(transform(columns, c -> '    `' || c.name || '` string'), ',' || chr(10))
    || chr(10) || ')' || chr(10)
    || 'ROW FORMAT DELIMITED FIELDS TERMINATED BY '',''' || chr(10)
    || 'STORED AS INPUTFORMAT ''org.apache.hadoop.mapred.TextInputFormat''' || chr(10)
    || 'OUTPUTFORMAT ''org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat''' || chr(10)
    || 'LOCATION ''s3://segment-warehouse-snowflake/{warehouse_id}/personas_{space_id}/{schema_name}/user_traits/'''
    AS ddl
FROM schema_reader_traits_{space_id}
WHERE "$path" LIKE '%schema.json'
LIMIT 1;
```

#### BigQuery — traits (swap `traits`→`identifiers` and `user_traits/`→`user_identifiers/` for the identifiers variant)

**Step 1 — schema reader:**
```sql
DROP TABLE IF EXISTS schema_reader_traits_{space_id};

CREATE EXTERNAL TABLE schema_reader_traits_{space_id} (
    bigquery array<struct<Name:string, Type:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://segment-warehouse-bigquery/{warehouse_id}/personas_{space_id}/{schema_name}/user_traits/';
```

**Step 2 — meta-query (iterates `bigquery` struct-array, not `columns`; emits JSON SerDe DDL):**
```sql
DROP TABLE IF EXISTS loader_traits_{space_id};

SELECT 'CREATE EXTERNAL TABLE loader_traits_{space_id} (' || chr(10)
    || array_join(transform(bigquery, c -> '    `' || c.Name || '` string'), ',' || chr(10))
    || chr(10) || ')' || chr(10)
    || 'ROW FORMAT SERDE ''org.openx.data.jsonserde.JsonSerDe''' || chr(10)
    || 'WITH SERDEPROPERTIES (''ignore.malformed.json'' = ''true'')' || chr(10)
    || 'STORED AS INPUTFORMAT ''org.apache.hadoop.mapred.TextInputFormat''' || chr(10)
    || 'OUTPUTFORMAT ''org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat''' || chr(10)
    || 'LOCATION ''s3://segment-warehouse-bigquery/{warehouse_id}/personas_{space_id}/{schema_name}/user_traits/'''
    AS ddl
FROM schema_reader_traits_{space_id}
WHERE "$path" LIKE '%schema.json'
LIMIT 1;
```

**BigQuery caveat:** review the generated DDL for duplicate columns (schema.json can list `received_at` twice); remove duplicates before running.

### Substitutions

When generating DDL for a user request, substitute all four tokens before emitting:

| token | source | example |
|---|---|---|
| `{space_id}` | user-provided space ID (with `spa_` prefix) | `spa_kYudtEBSHBDXhzR3xWhprZ` |
| `{warehouse_id}` | user-provided warehouse source ID | `ePokgNuAKyEdoLtWwa91fn` |
| `{schema_name}` | user-provided schema/project slug | `dig_engage_dev` |
| `{artifact}` | `traits` or `identifiers` (depends on which mismatch they're debugging) | `traits` |

The `SpaceID` is used verbatim as the table-name suffix (no transformation) — so the loader table for the example above is `loader_traits_spa_kYudtEBSHBDXhzR3xWhprZ`.

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
