# Patch V3 Athena Final Queries

All queries are meant to be run in **AWS Athena** against data stored in S3.

### S3 Buckets

| Bucket | Contents | Format |
|---|---|---|
| `segment-profiles` / `segment-profiles-stage` | V3 patches (source of truth) | JSON |
| `segment-warehouse-snowflake` / `segment-warehouse-snowflake-stage` | Snowflake warehouse loader output | CSV (`output.N.csv.gz`) + JSON (`merged.json.gz`) |

### Scope

These queries validate only the **materialized view (MV) tables**: `user_traits` and `user_identifiers`. Append-only (event) tables are too dynamic to validate with a single query that works across customers.

## 1. Create Loader Identifiers Table (Dynamic from schema.json)

This table is used by both the identifier inserts query (section 2) and the identifier deletes query (section 3).

**Substitute before running:**
- `<warehouse_id>` — warehouse source ID (e.g. `w5tevAWoyfNRmdisUcdavB`)
- `<space_id>` — Personas space ID with prefix (e.g. `spa_7dwJTm2tmQFEwcECEbYMu3`)
- `<schema-name>` — project slug / schema name (e.g. `personas_main_2`)
- `{space_short_id}` — short space ID for table naming (e.g. `spa_7dwJTm2tmQFEwcECEbYMu3`)

Step 1: Create a schema reader table pointing at the same S3 location:

**Snowflake:**

```sql
CREATE EXTERNAL TABLE schema_reader_identifiers_{space_short_id} (
    columns array<struct<name:string, type:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://segment-warehouse-snowflake/<warehouse_id>/personas_<space_id>/<schema-name>/user_identifiers/';
```

**BigQuery:**

BQ schema.json stores columns in a `bigquery` array (not `columns` object).

```sql
CREATE EXTERNAL TABLE schema_reader_identifiers_{space_short_id} (
    bigquery array<struct<Name:string, Type:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://segment-warehouse-bigquery/<warehouse_id>/personas_<space_id>/<schema-name>/user_identifiers/';
```

Step 2: Generate the full CREATE TABLE DDL dynamically (column order varies across spaces):

**Snowflake (CSV):**

```sql
SELECT 'CREATE EXTERNAL TABLE loader_identifiers_{space_short_id} (' || chr(10)
    || array_join(
        transform(columns, c -> '    `' || c.name || '` string'),
        ',' || chr(10)
    ) || chr(10)
    || ')' || chr(10)
    || 'ROW FORMAT DELIMITED FIELDS TERMINATED BY '',''' || chr(10)
    || 'STORED AS INPUTFORMAT ''org.apache.hadoop.mapred.TextInputFormat''' || chr(10)
    || 'OUTPUTFORMAT ''org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat''' || chr(10)
    || 'LOCATION ''s3://segment-warehouse-snowflake/<warehouse_id>/personas_<space_id>/<schema-name>/user_identifiers/'';'
    AS ddl
FROM schema_reader_identifiers_{space_short_id}
WHERE "$path" LIKE '%schema.json'
LIMIT 1;
```

**BigQuery (JSON):**

```sql
SELECT 'CREATE EXTERNAL TABLE loader_identifiers_{space_short_id} (' || chr(10)
    || array_join(
        transform(bigquery, c -> '    `' || c.Name || '` string'),
        ',' || chr(10)
    ) || chr(10)
    || ')' || chr(10)
    || 'ROW FORMAT SERDE ''org.openx.data.jsonserde.JsonSerDe''' || chr(10)
    || 'WITH SERDEPROPERTIES (''ignore.malformed.json'' = ''true'')' || chr(10)
    || 'STORED AS INPUTFORMAT ''org.apache.hadoop.mapred.TextInputFormat''' || chr(10)
    || 'OUTPUTFORMAT ''org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat''' || chr(10)
    || 'LOCATION ''s3://segment-warehouse-bigquery/<warehouse_id>/personas_<space_id>/<schema-name>/user_identifiers/'';'
    AS ddl
FROM schema_reader_identifiers_{space_short_id}
WHERE "$path" LIKE '%schema.json'
LIMIT 1;
```

Step 3: Review the generated DDL for duplicate columns (BQ schema.json may list `received_at` twice). Then run the DDL to create the loader table.

## 2. Identifier Inserts: Patches vs Loader Query

Notes:
- Excludes segments that were TOMBSTONED (within the tombstoned time window) — these were inserted and later deleted, so we don't need to validate their inserts. The tombstoned window should be at least 2 hours wider than the query window on each side, because if a create and tombstone happen in the same sync, the insert row is never written by the loader
- Excludes `acc_*` segments (account profiles are intentionally skipped by the loader)
- For MERGED patches, all identifiers pass through (the loader re-writes all identifiers with the MERGED patch's `accepted_at`); for other patch types, only `status = ADDED`
- **Links are also inserted as identifiers**: the loader converts each link on a patch into a synthetic identifier with `type = 'group_id'`, `id = link.group_id`. On non-MERGED patches only links with `status IN ('ADDED','CHANGED')` produce an insert (UNCHANGED links are skipped); on MERGED patches all links pass through
- The identifier ID is `SHA1(segment_id + type + id)`, matching what the loader produces (for links, `type = 'group_id'` and `id = link.group_id`)
- Both patches and loader sides use `row_number() OVER (PARTITION BY id ORDER BY accepted_at DESC)` to keep only the most recent version of each identifier — an identifier can be updated multiple times within the time window, and the warehouse only retains the latest value
- `__profile_version` is `accepted_at` in nanoseconds — the query converts it back for comparison
- Only the `loader` CTE differs between Snowflake and BigQuery — all other CTEs are identical

**Substitute before running:**
- `{space_id}`, `{day}`, `{start_at}`, `{end_at}`, `{tombstoned_start_at}`, `{tombstoned_end_at}` — in the `params` CTE
- `{loader_identifiers_table}` — the table name from section 1

**Snowflake (CSV):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at,
        '{tombstoned_start_at}' AS tombstoned_start_at,
        '{tombstoned_end_at}' AS tombstoned_end_at
),
merged_segments AS (
    SELECT DISTINCT d.segment_id
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary = 'TOMBSTONED'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.tombstoned_start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.tombstoned_end_at)
),
filtered_patches AS (
    SELECT d.*, d."$path" AS s3_path
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary IN ('NORMAL', 'CREATED', 'REEMIT', 'MERGED')
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.end_at)
        AND d.segment_id NOT IN (SELECT segment_id FROM merged_segments)
),
unnested_identifiers AS (
    SELECT p.segment_id AS canonical_segment_id, id.type,
        id.id AS value,
        lower(to_hex(sha1(to_utf8(p.segment_id || id.type || id.id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_patches p
        CROSS JOIN UNNEST(p.identifiers) AS t2(id)
    WHERE ((p.summary != 'MERGED' AND id.status = 'ADDED') OR (p.summary = 'MERGED'))
),
unnested_links AS (
    SELECT p.segment_id AS canonical_segment_id,
        'group_id' AS type,
        lnk.group_id AS value,
        lower(to_hex(sha1(to_utf8(p.segment_id || 'group_id' || lnk.group_id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_patches p
        CROSS JOIN UNNEST(p.links) AS t2(lnk)
    WHERE ((p.summary != 'MERGED' AND lnk.status IN ('ADDED', 'CHANGED')) OR (p.summary = 'MERGED'))
),
unnested_patches AS (
    SELECT canonical_segment_id, type, value, id, accepted_at, s3_path FROM unnested_identifiers
    UNION ALL
    SELECT canonical_segment_id, type, value, id, accepted_at, s3_path FROM unnested_links
),
patches AS (
    SELECT canonical_segment_id, type, value, id, accepted_at, s3_path
    FROM (
        SELECT *, row_number() OVER (PARTITION BY id ORDER BY accepted_at DESC) AS rn
        FROM unnested_patches
    ) WHERE rn = 1
),
loader AS (
    SELECT canonical_segment_id, type, lower(id) AS id, value, accepted_at
    FROM (
        SELECT l.canonical_segment_id, l.type, l.id, l.value,
            date_format(from_unixtime(CAST(CAST(l.__profile_version AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST((CAST(l.__profile_version AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at,
            row_number() OVER (PARTITION BY l.id ORDER BY CAST(l.__profile_version AS bigint) DESC) AS rn
        FROM {loader_identifiers_table} l, params p
        WHERE l."$path" LIKE '%output%csv.gz'
            AND (l.__deleted IS NULL OR l.__deleted = '' OR l.__deleted = 'false')
            AND CAST(l.__profile_version AS bigint) BETWEEN CAST(to_unixtime(from_iso8601_timestamp(p.start_at)) * 1000000000 AS bigint)
                AND CAST(to_unixtime(from_iso8601_timestamp(p.end_at)) * 1000000000 AS bigint)
    ) WHERE rn = 1
)
SELECT p.canonical_segment_id, p.type, p.value, p.id, p.accepted_at, p.s3_path
FROM patches p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

**BigQuery (JSON):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at,
        '{tombstoned_start_at}' AS tombstoned_start_at,
        '{tombstoned_end_at}' AS tombstoned_end_at
),
merged_segments AS (
    SELECT DISTINCT d.segment_id
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary = 'TOMBSTONED'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.tombstoned_start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.tombstoned_end_at)
),
filtered_patches AS (
    SELECT d.*, d."$path" AS s3_path
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary IN ('NORMAL', 'CREATED', 'REEMIT', 'MERGED')
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.end_at)
        AND d.segment_id NOT IN (SELECT segment_id FROM merged_segments)
),
unnested_identifiers AS (
    SELECT p.segment_id AS canonical_segment_id, id.type,
        id.id AS value,
        lower(to_hex(sha1(to_utf8(p.segment_id || id.type || id.id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_patches p
        CROSS JOIN UNNEST(p.identifiers) AS t2(id)
    WHERE ((p.summary != 'MERGED' AND id.status = 'ADDED') OR (p.summary = 'MERGED'))
),
unnested_links AS (
    SELECT p.segment_id AS canonical_segment_id,
        'group_id' AS type,
        lnk.group_id AS value,
        lower(to_hex(sha1(to_utf8(p.segment_id || 'group_id' || lnk.group_id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_patches p
        CROSS JOIN UNNEST(p.links) AS t2(lnk)
    WHERE ((p.summary != 'MERGED' AND lnk.status IN ('ADDED', 'CHANGED')) OR (p.summary = 'MERGED'))
),
unnested_patches AS (
    SELECT canonical_segment_id, type, value, id, accepted_at, s3_path FROM unnested_identifiers
    UNION ALL
    SELECT canonical_segment_id, type, value, id, accepted_at, s3_path FROM unnested_links
),
patches AS (
    SELECT canonical_segment_id, type, value, id, accepted_at, s3_path
    FROM (
        SELECT *, row_number() OVER (PARTITION BY id ORDER BY accepted_at DESC) AS rn
        FROM unnested_patches
    ) WHERE rn = 1
),
loader AS (
    SELECT canonical_segment_id, type, lower(id) AS id, value, accepted_at
    FROM (
        SELECT l.canonical_segment_id, l.type, l.id, l.value,
            date_format(from_unixtime(CAST(CAST(l."__profile_version" AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST((CAST(l."__profile_version" AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at,
            row_number() OVER (PARTITION BY l.id ORDER BY CAST(l."__profile_version" AS bigint) DESC) AS rn
        FROM {loader_identifiers_table} l, params p
        WHERE l."$path" LIKE '%output%.json.gz'
            AND (l."__deleted" IS NULL OR l."__deleted" = 'false')
            AND CAST(l."__profile_version" AS bigint) BETWEEN CAST(to_unixtime(from_iso8601_timestamp(p.start_at)) * 1000000000 AS bigint)
                AND CAST(to_unixtime(from_iso8601_timestamp(p.end_at)) * 1000000000 AS bigint)
    ) WHERE rn = 1
)
SELECT p.canonical_segment_id, p.type, p.value, p.id, p.accepted_at, p.s3_path
FROM patches p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

## 3. Identifier Deletes: Patches vs Loader Query

Uses the same loader table created in section 1.

Notes:
- Only validates MERGED patch deletes for now — identifier deletes from non-merge patches (`cause = 'CUSTOMER_DISASSOCIATED'` or `cause = 'MERGED'` on non-merge summaries) are not covered
- The loader generates a delete marker for every `from_segment_id` × every identifier on the patch (cartesian product). This produces many redundant deletes (identifiers that never existed on the losing segment), but that is the current loader logic
- Additionally, the loader generates a delete marker for every `from_segment_id` × every link (group_id) on the patch
- For identifiers: `id = SHA1(from_segment_id + type + identifier_id)` — using the losing segment
- For links: `id = SHA1(from_segment_id + 'group_id' + group_id)`
- `canonical_segment_id` is the winning `segment_id`
- No `row_number()` partitioning needed — unlike inserts, each delete event is generated exactly once
- Uses LEFT JOIN + WHERE NULL instead of EXCEPT to expose the S3 path of unmatched patches for debugging
- Only the `loader` CTE differs between Snowflake and BigQuery — all other CTEs are identical
- **False positives from INSERT / UPDATE + TOMBSTONED in quick succession**: If a segment receives a INSERT / UPDATE patch and then a TOMBSTONED patch within the same sync window, the loader may never write the expected delete markers (since it sees both and skips the segment entirely). These will show up as mismatches. To verify that unmatched rows fall into this case, use the investigation query below to check if the segment had a TOMBSTONED patch shortly after the MERGED patch

**Investigation query for false positives** — for each unmatched `segment_id`, check if it was TOMBSTONED shortly after being MERGED:

```sql
SELECT *
FROM data_v3 d
WHERE d.namespace_id = '{space_id}'
    AND d.day_written = CAST(CAST(to_unixtime(date_parse('{day}', '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
    AND d.segment_id = '{segment_id}'
ORDER BY d.accepted_at ASC;
```

If you see an insert or update patch followed closely by a TOMBSTONED for the same segment ID (i.e. within a short time interval), the mismatch is expected — the loader sees both events in the same sync window and skips the segment entirely, so it never writes the expected delete markers.

**Substitute before running:**
- `{space_id}`, `{day}`, `{start_at}`, `{end_at}` — in the `params` CTE
- `{loader_identifiers_table}` — the table name from section 1

**Snowflake (CSV):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at
),
filtered_merged AS (
    SELECT d.*, d."$path" AS s3_path
    FROM data_v3 d
    WHERE d.namespace_id = (SELECT space_id FROM params)
        AND d.day_written = CAST(CAST(to_unixtime(date_parse((SELECT day FROM params), '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary = 'MERGED'
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp((SELECT start_at FROM params))
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp((SELECT end_at FROM params))
),
identifier_deletes AS (
    SELECT p.segment_id AS canonical_segment_id,
        lower(to_hex(sha1(to_utf8(m.from_segment_id || id.type || id.id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_merged p
        CROSS JOIN UNNEST(p.merges) AS t(m)
        CROSS JOIN UNNEST(p.identifiers) AS t2(id)
),
link_deletes AS (
    SELECT p.segment_id AS canonical_segment_id,
        lower(to_hex(sha1(to_utf8(m.from_segment_id || 'group_id' || lnk.group_id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_merged p
        CROSS JOIN UNNEST(p.merges) AS t(m)
        CROSS JOIN UNNEST(p.links) AS t2(lnk)
),
expected_deletes AS (
    SELECT canonical_segment_id, id, accepted_at, s3_path FROM identifier_deletes
    UNION
    SELECT canonical_segment_id, id, accepted_at, s3_path FROM link_deletes
),
loader AS (
    SELECT canonical_segment_id, lower(id) AS id, type, value,
        date_format(from_unixtime(CAST(CAST(l.__profile_version AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST((CAST(l.__profile_version AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at
    FROM {loader_identifiers_table} l
    WHERE l."$path" LIKE '%output%csv.gz'
        AND l.__deleted = 'true'
        AND CAST(l.__profile_version AS bigint) BETWEEN CAST(
            to_unixtime(from_iso8601_timestamp((SELECT start_at FROM params))) * 1000000000 AS bigint
        )
        AND CAST(
            to_unixtime(from_iso8601_timestamp((SELECT end_at FROM params))) * 1000000000 AS bigint
        )
)
SELECT p.canonical_segment_id, p.id, p.accepted_at, p.s3_path
FROM expected_deletes p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

**BigQuery (JSON):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at
),
filtered_merged AS (
    SELECT d.*, d."$path" AS s3_path
    FROM data_v3 d
    WHERE d.namespace_id = (SELECT space_id FROM params)
        AND d.day_written = CAST(CAST(to_unixtime(date_parse((SELECT day FROM params), '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary = 'MERGED'
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp((SELECT start_at FROM params))
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp((SELECT end_at FROM params))
),
identifier_deletes AS (
    SELECT p.segment_id AS canonical_segment_id,
        lower(to_hex(sha1(to_utf8(m.from_segment_id || id.type || id.id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_merged p
        CROSS JOIN UNNEST(p.merges) AS t(m)
        CROSS JOIN UNNEST(p.identifiers) AS t2(id)
),
link_deletes AS (
    SELECT p.segment_id AS canonical_segment_id,
        lower(to_hex(sha1(to_utf8(m.from_segment_id || 'group_id' || lnk.group_id)))) AS id,
        date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        p.s3_path
    FROM filtered_merged p
        CROSS JOIN UNNEST(p.merges) AS t(m)
        CROSS JOIN UNNEST(p.links) AS t2(lnk)
),
expected_deletes AS (
    SELECT canonical_segment_id, id, accepted_at, s3_path FROM identifier_deletes
    UNION
    SELECT canonical_segment_id, id, accepted_at, s3_path FROM link_deletes
),
loader AS (
    SELECT canonical_segment_id, lower(id) AS id,
        date_format(from_unixtime(CAST(CAST(l."__profile_version" AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
        LPAD(CAST((CAST(l."__profile_version" AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at
    FROM {loader_identifiers_table} l
    WHERE l."$path" LIKE '%output%.json.gz'
        AND l."__deleted" = 'true'
        AND CAST(l."__profile_version" AS bigint) BETWEEN CAST(
            to_unixtime(from_iso8601_timestamp((SELECT start_at FROM params))) * 1000000000 AS bigint
        )
        AND CAST(
            to_unixtime(from_iso8601_timestamp((SELECT end_at FROM params))) * 1000000000 AS bigint
        )
)
SELECT p.canonical_segment_id, p.id, p.accepted_at, p.s3_path
FROM expected_deletes p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

## 5. Create Loader Traits Table (Dynamic from schema.json)

This table is used by both the trait inserts query (section 6) and the trait deletes query (section 7).

Step 1: Create a schema reader table pointing at the traits S3 location:

**Snowflake:**

```sql
drop table if exists schema_reader_traits_{space_short_id};

CREATE EXTERNAL TABLE schema_reader_traits_{space_short_id} (
    columns array<struct<name:string, type:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://segment-warehouse-snowflake/<warehouse_id>/personas_<space_id>/<schema-name>/user_traits/';
```

**BigQuery:**

BQ schema.json stores columns in a `bigquery` array (not `columns` object).

```sql
drop table if exists schema_reader_traits_{space_short_id};

CREATE EXTERNAL TABLE schema_reader_traits_{space_short_id} (
    bigquery array<struct<Name:string, Type:string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://segment-warehouse-bigquery/<warehouse_id>/personas_<space_id>/<schema-name>/user_traits/';
```

Step 2: Generate the full CREATE TABLE DDL dynamically (column order varies across spaces):

**Snowflake (CSV):**

```sql
SELECT 'CREATE EXTERNAL TABLE loader_traits_{space_short_id} (' || chr(10)
    || array_join(
        transform(columns, c -> '    `' || c.name || '` string'),
        ',' || chr(10)
    ) || chr(10)
    || ')' || chr(10)
    || 'ROW FORMAT DELIMITED FIELDS TERMINATED BY '',''' || chr(10)
    || 'STORED AS INPUTFORMAT ''org.apache.hadoop.mapred.TextInputFormat''' || chr(10)
    || 'OUTPUTFORMAT ''org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat''' || chr(10)
    || 'LOCATION ''s3://segment-warehouse-snowflake/<warehouse_id>/personas_<space_id>/<schema-name>/user_traits/'';'
    AS ddl
FROM schema_reader_traits_{space_short_id}
WHERE "$path" LIKE '%schema.json'
LIMIT 1;
```

**BigQuery (JSON):**

```sql
SELECT 'CREATE EXTERNAL TABLE loader_traits_{space_short_id} (' || chr(10)
    || array_join(
        transform(bigquery, c -> '    `' || c.Name || '` string'),
        ',' || chr(10)
    ) || chr(10)
    || ')' || chr(10)
    || 'ROW FORMAT SERDE ''org.openx.data.jsonserde.JsonSerDe''' || chr(10)
    || 'WITH SERDEPROPERTIES (''ignore.malformed.json'' = ''true'')' || chr(10)
    || 'STORED AS INPUTFORMAT ''org.apache.hadoop.mapred.TextInputFormat''' || chr(10)
    || 'OUTPUTFORMAT ''org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat''' || chr(10)
    || 'LOCATION ''s3://segment-warehouse-bigquery/<warehouse_id>/personas_<space_id>/<schema-name>/user_traits/'';'
    AS ddl
FROM schema_reader_traits_{space_short_id}
WHERE "$path" LIKE '%schema.json'
LIMIT 1;
```

Step 3: Review the generated DDL — BQ schema.json may list `received_at` twice; remove any duplicate columns. Then run the DDL to create the loader table.

## 6. Trait Inserts: Patches vs Loader Query

Notes:
- Same pattern as identifiers query (#2): TOMBSTONED exclusion with separate time params, MERGED included
- For MERGED patches, all traits pass through; for others, any trait whose `status != 'UNCHANGED'` with a non-null `curr_value` produces an insert. The loader derives Created/Updated purely from prev/curr being nil — a `CONFLICT` row with a non-null curr_value also emits an insert
- The SQL filter is `tr.curr_value IS NOT NULL` only. An empty string (`''`) or the literal 4-char string `'null'` are valid user values and **do** produce an insert (they correspond to `IsZero() = false` in `patchv3.Dynamic`). Only SQL `NULL` (which is how the Dynamic-null sentinel lands in Athena) should be excluded
- Trait key normalization matches `go-snakecase`: non-alphanumeric → `_`, camelCase split, collapse `__`, strip leading `_`, lowercase
- Both the patches and loader sides use `row_number() OVER (PARTITION BY id ORDER BY accepted_at DESC)` to keep only the most recent version of each trait — a trait can be updated multiple times within the time window, and the warehouse only retains the latest value
- Only the `loader` CTE differs between Snowflake and BigQuery — all other CTEs are identical

**Snowflake (CSV):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at,
        '{tombstoned_start_at}' AS tombstoned_start_at,
        '{tombstoned_end_at}' AS tombstoned_end_at
),
merged_segments AS (
    SELECT DISTINCT d.segment_id
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary = 'TOMBSTONED'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.tombstoned_start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.tombstoned_end_at)
),
filtered_patches AS (
    SELECT d.*, d."$path" AS s3_path
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary IN ('NORMAL', 'CREATED', 'REEMIT', 'MERGED')
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.end_at)
        AND d.segment_id NOT IN (SELECT segment_id FROM merged_segments)
),
normalize_key AS (
    SELECT *, lower(regexp_replace(regexp_replace(
        regexp_replace(regexp_replace(
            regexp_replace(raw_key, '[^a-zA-Z0-9]', '_'),
        '([A-Z]+)([A-Z][a-z])', '$1_$2'),
        '([a-z0-9])([A-Z])', '$1_$2'), '_{2,}', '_'), '^_', '')) AS trait_key
    FROM (
        SELECT p.segment_id AS canonical_segment_id, tr.key AS raw_key,
            date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
            p.s3_path
        FROM filtered_patches p
            CROSS JOIN UNNEST(p.traits) AS t(tr)
        WHERE ((p.summary != 'MERGED' AND tr.status != 'UNCHANGED' AND tr.curr_value IS NOT NULL) OR (p.summary = 'MERGED'))
    )
),
unnested_traits AS (
    SELECT canonical_segment_id,
        lower(to_hex(sha1(to_utf8(canonical_segment_id || trait_key)))) AS id,
        trait_key AS name,
        accepted_at, s3_path
    FROM normalize_key
),
patches AS (
    SELECT canonical_segment_id, id, name, accepted_at, s3_path
    FROM (
        SELECT *, row_number() OVER (PARTITION BY id ORDER BY accepted_at DESC) AS rn
        FROM unnested_traits
    ) WHERE rn = 1
),
loader AS (
    SELECT canonical_segment_id, lower(id) AS id, name, accepted_at
    FROM (
        SELECT l.canonical_segment_id, l.id, l.name,
            date_format(from_unixtime(CAST(CAST(l.__profile_version AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST((CAST(l.__profile_version AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at,
            row_number() OVER (PARTITION BY l.id ORDER BY CAST(l.__profile_version AS bigint) DESC) AS rn
        FROM {loader_traits_table} l, params p
        WHERE l."$path" LIKE '%output%csv.gz'
            AND (l.__deleted IS NULL OR l.__deleted = '' OR l.__deleted = 'false')
            AND CAST(l.__profile_version AS bigint) BETWEEN CAST(to_unixtime(from_iso8601_timestamp(p.start_at)) * 1000000000 AS bigint)
                AND CAST(to_unixtime(from_iso8601_timestamp(p.end_at)) * 1000000000 AS bigint)
    ) WHERE rn = 1
)
SELECT p.canonical_segment_id, p.id, p.name, p.accepted_at, p.s3_path
FROM patches p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

**BigQuery (JSON):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at,
        '{tombstoned_start_at}' AS tombstoned_start_at,
        '{tombstoned_end_at}' AS tombstoned_end_at
),
merged_segments AS (
    SELECT DISTINCT d.segment_id
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary = 'TOMBSTONED'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.tombstoned_start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.tombstoned_end_at)
),
filtered_patches AS (
    SELECT d.*, d."$path" AS s3_path
    FROM data_v3 d, params p
    WHERE d.namespace_id = p.space_id
        AND d.day_written = CAST(CAST(to_unixtime(date_parse(p.day, '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary IN ('NORMAL', 'CREATED', 'REEMIT', 'MERGED')
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp(p.start_at)
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp(p.end_at)
        AND d.segment_id NOT IN (SELECT segment_id FROM merged_segments)
),
normalize_key AS (
    SELECT *, lower(regexp_replace(regexp_replace(
        regexp_replace(regexp_replace(
            regexp_replace(raw_key, '[^a-zA-Z0-9]', '_'),
        '([A-Z]+)([A-Z][a-z])', '$1_$2'),
        '([a-z0-9])([A-Z])', '$1_$2'), '_{2,}', '_'), '^_', '')) AS trait_key
    FROM (
        SELECT p.segment_id AS canonical_segment_id, tr.key AS raw_key,
            date_format(from_iso8601_timestamp(p.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST(millisecond(from_iso8601_timestamp(p.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
            p.s3_path
        FROM filtered_patches p
            CROSS JOIN UNNEST(p.traits) AS t(tr)
        WHERE ((p.summary != 'MERGED' AND tr.status != 'UNCHANGED' AND tr.curr_value IS NOT NULL) OR (p.summary = 'MERGED'))
    )
),
unnested_traits AS (
    SELECT canonical_segment_id,
        lower(to_hex(sha1(to_utf8(canonical_segment_id || trait_key)))) AS id,
        trait_key AS name,
        accepted_at, s3_path
    FROM normalize_key
),
patches AS (
    SELECT canonical_segment_id, id, name, accepted_at, s3_path
    FROM (
        SELECT *, row_number() OVER (PARTITION BY id ORDER BY accepted_at DESC) AS rn
        FROM unnested_traits
    ) WHERE rn = 1
),
loader AS (
    SELECT canonical_segment_id, lower(id) AS id, name, accepted_at
    FROM (
        SELECT l.canonical_segment_id, l.id, l.name,
            date_format(from_unixtime(CAST(CAST(l."__profile_version" AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST((CAST(l."__profile_version" AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at,
            row_number() OVER (PARTITION BY l.id ORDER BY CAST(l."__profile_version" AS bigint) DESC) AS rn
        FROM {loader_traits_table} l, params p
        WHERE l."$path" LIKE '%output%.json.gz'
            AND (l."__deleted" IS NULL OR l."__deleted" = 'false')
            AND CAST(l."__profile_version" AS bigint) BETWEEN CAST(to_unixtime(from_iso8601_timestamp(p.start_at)) * 1000000000 AS bigint)
                AND CAST(to_unixtime(from_iso8601_timestamp(p.end_at)) * 1000000000 AS bigint)
    ) WHERE rn = 1
)
SELECT p.canonical_segment_id, p.id, p.name, p.accepted_at, p.s3_path
FROM patches p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

## 7. Trait Deletes: Patches vs Loader Query

Uses the same loader table created in section 5.

Notes:
- Handles **two types** of trait deletions:
  - **Changed deletes**: Non-merge patches (NORMAL, CREATED, REEMIT) with `status != 'UNCHANGED'` and a non-null `prev_value` — the loader writes a delete marker regardless of `curr_value` (so a trait being cleared to null/empty also produces a delete). An empty string (`''`) or the literal string `'null'` are valid `prev_value`s and **do** produce a delete
  - **Merged deletes**: MERGED patches — the loader generates a delete marker for every `from_segment_id` × every trait on the patch (cartesian product). This produces many redundant deletes (traits that never existed on the losing segment), but that is the current loader logic
- Trait key normalization matches `go-snakecase`: non-alphanumeric → `_`, camelCase split, collapse `__`, strip leading `_`, lowercase
- For changed deletes: `id = SHA1(segment_id + normalized_key)`
- For merged deletes: `id = SHA1(from_segment_id + normalized_key)`, but `canonical_segment_id` is the winning `segment_id`
- No `row_number()` partitioning needed — unlike inserts, each delete event is generated exactly once (there is no "latest version" to pick)
- Only the `loader` CTE differs between Snowflake and BigQuery — all other CTEs are identical
- **False positives from MERGED + TOMBSTONED in quick succession**: If a segment receives a MERGED patch and then a TOMBSTONED patch within the same sync window, the loader may never write the expected delete markers (since it sees both and skips the segment entirely). These will show up as mismatches. To verify that unmatched rows fall into this case, use the investigation query below to check if the segment had a TOMBSTONED patch shortly after the MERGED patch

**Investigation query for false positives** — for each unmatched `segment_id`, check its traits and patch history:

```sql
SELECT *
FROM data_v3 d
WHERE d.namespace_id = '{space_id}'
    AND d.day_written = CAST(CAST(to_unixtime(date_parse('{day}', '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
    AND d.segment_id = '{segment_id}'
ORDER BY d.accepted_at ASC;
```

If you see an insert or update patch followed closely by a TOMBSTONED for the same segment ID (i.e. within a short time interval), the mismatch is expected — the loader sees both events in the same sync window and skips the segment entirely, so it never writes the expected delete markers.

**Substitute before running:**
- `{space_id}`, `{day}`, `{start_at}`, `{end_at}` — in the `params` CTE
- `{loader_traits_table}` — the table name from section 5

**Snowflake (CSV):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at
),
filter_patches AS (
    SELECT d.*,
        d."$path" AS s3_path
    FROM data_v3 d
    WHERE d.namespace_id = (SELECT space_id FROM params)
        AND d.day_written = CAST(CAST(to_unixtime(date_parse((SELECT day FROM params), '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary IN ('NORMAL', 'CREATED', 'REEMIT', 'MERGED')
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp((SELECT start_at FROM params))
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp((SELECT end_at FROM params))
),
normalize_key AS (
    SELECT
        p.segment_id,
        p.summary,
        p.accepted_at,
        p.s3_path,
        tr.key AS raw_key,
        tr.status AS trait_status,
        tr.prev_value,
        tr.curr_value,
        lower(regexp_replace(regexp_replace(
            regexp_replace(regexp_replace(
                regexp_replace(tr.key, '[^a-zA-Z0-9]', '_'),
            '([A-Z]+)([A-Z][a-z])', '$1_$2'),
            '([a-z0-9])([A-Z])', '$1_$2'), '_{2,}', '_'), '^_', '')) AS trait_key
    FROM filter_patches p
        CROSS JOIN UNNEST(p.traits) AS t2(tr)
),
changed_deletes AS (
    SELECT
        segment_id AS canonical_segment_id,
        raw_key,
        curr_value,
        lower(to_hex(sha1(to_utf8(segment_id || trait_key)))) AS id,
        date_format(from_iso8601_timestamp(accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST(millisecond(from_iso8601_timestamp(accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        s3_path
    FROM normalize_key
    WHERE summary IN ('NORMAL', 'CREATED', 'REEMIT')
        AND trait_status != 'UNCHANGED'
        AND prev_value IS NOT NULL
),
merged_deletes AS (
    SELECT
        nk.segment_id AS canonical_segment_id,
        nk.raw_key,
        nk.curr_value,
        lower(to_hex(sha1(to_utf8(m.from_segment_id || nk.trait_key)))) AS id,
        date_format(from_iso8601_timestamp(nk.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST(millisecond(from_iso8601_timestamp(nk.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        nk.s3_path
    FROM normalize_key nk
        JOIN filter_patches fp ON nk.segment_id = fp.segment_id AND nk.accepted_at = fp.accepted_at
        CROSS JOIN UNNEST(fp.merges) AS t(m)
    WHERE nk.summary = 'MERGED'
),
all_expected_deletes AS (
    SELECT canonical_segment_id, raw_key, curr_value, id, accepted_at, s3_path FROM changed_deletes
    UNION
    SELECT canonical_segment_id, raw_key, curr_value, id, accepted_at, s3_path FROM merged_deletes
),
loader AS (
    SELECT canonical_segment_id, lower(id) AS id,
        date_format(from_unixtime(CAST(CAST(l.__profile_version AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST((CAST(l.__profile_version AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at
    FROM {loader_traits_table} l
    WHERE l."$path" LIKE '%output%csv.gz'
        AND l.__deleted = 'true'
        AND CAST(l.__profile_version AS bigint) BETWEEN CAST(
            to_unixtime(from_iso8601_timestamp((SELECT start_at FROM params))) * 1000000000 AS bigint
        )
        AND CAST(
            to_unixtime(from_iso8601_timestamp((SELECT end_at FROM params))) * 1000000000 AS bigint
        )
)
SELECT p.canonical_segment_id, p.raw_key, p.curr_value, p.id, p.accepted_at, p.s3_path
FROM all_expected_deletes p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

**BigQuery (JSON):**

```sql
WITH params AS (
    SELECT '{space_id}' AS space_id,
        '{day}' AS day,
        '{start_at}' AS start_at,
        '{end_at}' AS end_at
),
filter_patches AS (
    SELECT d.*,
        d."$path" AS s3_path
    FROM data_v3 d
    WHERE d.namespace_id = (SELECT space_id FROM params)
        AND d.day_written = CAST(CAST(to_unixtime(date_parse((SELECT day FROM params), '%Y-%m-%d')) * 1000 AS BIGINT) AS VARCHAR)
        AND d.summary IN ('NORMAL', 'CREATED', 'REEMIT', 'MERGED')
        AND d.segment_id NOT LIKE 'acc_%'
        AND from_iso8601_timestamp(d.accepted_at) >= from_iso8601_timestamp((SELECT start_at FROM params))
        AND from_iso8601_timestamp(d.accepted_at) < from_iso8601_timestamp((SELECT end_at FROM params))
),
normalize_key AS (
    SELECT
        p.segment_id,
        p.summary,
        p.accepted_at,
        p.s3_path,
        tr.key AS raw_key,
        tr.status AS trait_status,
        tr.prev_value,
        tr.curr_value,
        lower(regexp_replace(regexp_replace(
            regexp_replace(regexp_replace(
                regexp_replace(tr.key, '[^a-zA-Z0-9]', '_'),
            '([A-Z]+)([A-Z][a-z])', '$1_$2'),
            '([a-z0-9])([A-Z])', '$1_$2'), '_{2,}', '_'), '^_', '')) AS trait_key
    FROM filter_patches p
        CROSS JOIN UNNEST(p.traits) AS t2(tr)
),
changed_deletes AS (
    SELECT
        segment_id AS canonical_segment_id,
        raw_key,
        curr_value,
        lower(to_hex(sha1(to_utf8(segment_id || trait_key)))) AS id,
        date_format(from_iso8601_timestamp(accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST(millisecond(from_iso8601_timestamp(accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        s3_path
    FROM normalize_key
    WHERE summary IN ('NORMAL', 'CREATED', 'REEMIT')
        AND trait_status != 'UNCHANGED'
        AND prev_value IS NOT NULL
),
merged_deletes AS (
    SELECT
        nk.segment_id AS canonical_segment_id,
        nk.raw_key,
        nk.curr_value,
        lower(to_hex(sha1(to_utf8(m.from_segment_id || nk.trait_key)))) AS id,
        date_format(from_iso8601_timestamp(nk.accepted_at), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST(millisecond(from_iso8601_timestamp(nk.accepted_at)) AS varchar), 3, '0') || 'Z' AS accepted_at,
        nk.s3_path
    FROM normalize_key nk
        JOIN filter_patches fp ON nk.segment_id = fp.segment_id AND nk.accepted_at = fp.accepted_at
        CROSS JOIN UNNEST(fp.merges) AS t(m)
    WHERE nk.summary = 'MERGED'
),
all_expected_deletes AS (
    SELECT canonical_segment_id, raw_key, curr_value, id, accepted_at, s3_path FROM changed_deletes
    UNION
    SELECT canonical_segment_id, raw_key, curr_value, id, accepted_at, s3_path FROM merged_deletes
),
loader AS (
    SELECT canonical_segment_id, lower(id) AS id,
        date_format(from_unixtime(CAST(CAST(l."__profile_version" AS bigint) / 1000000000 AS double)), '%Y-%m-%dT%H:%i:%s.') ||
            LPAD(CAST((CAST(l."__profile_version" AS bigint) / 1000000) % 1000 AS varchar), 3, '0') || 'Z' AS accepted_at
    FROM {loader_traits_table} l
    WHERE l."$path" LIKE '%output%.json.gz'
        AND l."__deleted" = 'true'
        AND CAST(l."__profile_version" AS bigint) BETWEEN CAST(
            to_unixtime(from_iso8601_timestamp((SELECT start_at FROM params))) * 1000000000 AS bigint
        )
        AND CAST(
            to_unixtime(from_iso8601_timestamp((SELECT end_at FROM params))) * 1000000000 AS bigint
        )
)
SELECT p.canonical_segment_id, p.raw_key, p.curr_value, p.id, p.accepted_at, p.s3_path
FROM all_expected_deletes p
    LEFT JOIN loader l ON p.id = l.id AND p.accepted_at = l.accepted_at
WHERE l.id IS NULL;
```

## Known Edge Cases and Caveats

1. **Merge deletes are a cartesian product (identifiers and traits)**: When the loader processes a MERGED patch, it generates a delete marker for every `from_segment_id` x every identifier (or trait) on the patch. This is a cartesian product — it does not check whether the identifier or trait actually belonged to the losing segment. Most of these deletes are redundant (the identifier/trait may have never existed on the losing segment), but this is the current loader behavior. The validation queries replicate this logic to match.

2. **Trait key camelCase-to-snake_case conversion does not fully replicate the Go library**: The loader uses the `go-snakecase` Go library to normalize trait keys (e.g., `firstName` -> `first_name`). The Athena queries approximate this with a series of `regexp_replace` calls, but regex-based conversion cannot handle all edge cases that the Go library handles (e.g., sequences of uppercase letters followed by lowercase, certain Unicode characters, or unusual mixed-case patterns). If a trait key mismatch is found, verify the normalized key manually against the Go library output before concluding it is a real discrepancy.

3. **Trait insert/delete filters track `patchv3.Dynamic.IsZero()`**: The loader's decision to emit an insert or delete row comes from `NewTraitFromPatch` in `warehouse-connectors/downloader/profiles/trait.go`. Traits with `Status == UNCHANGED` are dropped at the `ProfileChanges` split (`patch.go`). Otherwise, Created/Updated/Deleted is derived purely from whether the decoded prev/curr values are `nil`. In `patchv3.Dynamic`, `IsZero()` is true only when the bytes are empty or encode semantic null (JSON `null` / CBOR `0xf6`). JSON `""` and the 4-char string `"null"` are both valid non-nil values and produce an emission. Accordingly, the validation SQL filters on `curr_value IS NOT NULL` (for inserts) and `prev_value IS NOT NULL` (for deletes) only — it must **not** add `!= ''` or `!= 'null'`, as those would drop real user values that the loader did write.

4. **Link deletes in identifier delete validation**: The identifier deletes query (section 3) validates not only identifier delete markers but also **link delete markers**. For MERGED patches, the loader generates a delete marker for every `from_segment_id` x every link (`group_id`) on the patch, using `id = SHA1(from_segment_id + 'group_id' + group_id)`. These link deletes are included in the `expected_deletes` CTE alongside identifier deletes and are matched against the same loader identifiers table (since the loader writes both identifier and link delete markers to the `user_identifiers` output).

5. **Link inserts in identifier insert validation**: The identifier inserts query (section 2) validates not only identifier inserts but also **link inserts**. The loader converts each link on a patch into a synthetic identifier with `type = 'group_id'`, `id = link.group_id`, producing `id = SHA1(segment_id + 'group_id' + group_id)` in the warehouse. On non-MERGED patches only links with `status IN ('ADDED','CHANGED')` are written (UNCHANGED/unknown are skipped — see `downloader/profiles/patch.go` in `warehouse-connectors`). On MERGED patches all links pass through, mirroring identifier behavior. The `unnested_links` CTE is UNION ALL'd with `unnested_identifiers` into `unnested_patches`, and the same `row_number()` dedup over `id` keeps only the latest version.
