package validator

// WarehouseTarget describes the warehouse-loader S3 layout and Athena-query
// dialect differences (bucket, file pattern, schema.json shape, quoted columns).
// It owns all path-level information; ArtifactType contributes the leaf suffix.
type WarehouseTarget interface {
	Kind() WarehouseKind
	Bucket() string

	// OutputPathPattern is matched against the Athena "$path" column to restrict
	// the scan to data files (not schema.json, not metadata, not other outputs).
	OutputPathPattern() string

	// DeletedCol is the column identifier used in SQL to reference the
	// __deleted column — Snowflake uses bare __deleted; BigQuery uses "__deleted".
	DeletedCol() string

	// ProfileVersionCol is the column identifier used to reference __profile_version.
	ProfileVersionCol() string

	// DeletedNotTrue is the SQL predicate for "row is not a delete marker".
	// CSV loaders may emit empty strings; BigQuery JSON only emits null or "false".
	DeletedNotTrue() string

	// SchemaReaderSQL renders the schema_reader CREATE EXTERNAL TABLE DDL.
	SchemaReaderSQL(a ArtifactType, r ValidationRun) (string, error)

	// GenerateDDLSQL renders the meta-query that produces the loader table DDL
	// string. The returned DDL is then executed via Exec.
	GenerateDDLSQL(a ArtifactType, r ValidationRun) (string, error)
}

func NewWarehouseTarget(kind WarehouseKind) (WarehouseTarget, error) {
	switch kind {
	case WarehouseSnowflake:
		return &snowflakeTarget{}, nil
	case WarehouseBigQuery:
		return &bigqueryTarget{}, nil
	default:
		return nil, &SystemError{Step: "new_warehouse_target", Err: errUnknownWarehouse(kind)}
	}
}

type errUnknownWarehouse WarehouseKind

func (e errUnknownWarehouse) Error() string {
	return "unknown warehouse kind: " + string(e)
}

// --- Snowflake ---

type snowflakeTarget struct{}

func (snowflakeTarget) Kind() WarehouseKind      { return WarehouseSnowflake }
func (snowflakeTarget) Bucket() string           { return "segment-warehouse-snowflake" }
func (snowflakeTarget) OutputPathPattern() string { return "%output%csv.gz" }
func (snowflakeTarget) DeletedCol() string       { return "__deleted" }
func (snowflakeTarget) ProfileVersionCol() string { return "__profile_version" }
func (snowflakeTarget) DeletedNotTrue() string {
	return "(l.__deleted IS NULL OR l.__deleted = '' OR l.__deleted = 'false')"
}

func (t snowflakeTarget) SchemaReaderSQL(a ArtifactType, r ValidationRun) (string, error) {
	return renderTemplate(
		"schema_reader_"+string(a.Kind())+"_snowflake.sql.tmpl",
		schemaReaderData(t, a, r),
	)
}

func (t snowflakeTarget) GenerateDDLSQL(a ArtifactType, r ValidationRun) (string, error) {
	return renderTemplate(
		"generate_ddl_"+string(a.Kind())+"_snowflake.sql.tmpl",
		generateDDLData(t, a, r),
	)
}

// --- BigQuery ---

type bigqueryTarget struct{}

func (bigqueryTarget) Kind() WarehouseKind      { return WarehouseBigQuery }
func (bigqueryTarget) Bucket() string           { return "segment-warehouse-bigquery" }
func (bigqueryTarget) OutputPathPattern() string { return "%output%.json.gz" }
func (bigqueryTarget) DeletedCol() string       { return `"__deleted"` }
func (bigqueryTarget) ProfileVersionCol() string { return `"__profile_version"` }
func (bigqueryTarget) DeletedNotTrue() string {
	return `(l."__deleted" IS NULL OR l."__deleted" = 'false')`
}

func (t bigqueryTarget) SchemaReaderSQL(a ArtifactType, r ValidationRun) (string, error) {
	return renderTemplate(
		"schema_reader_"+string(a.Kind())+"_bigquery.sql.tmpl",
		schemaReaderData(t, a, r),
	)
}

func (t bigqueryTarget) GenerateDDLSQL(a ArtifactType, r ValidationRun) (string, error) {
	return renderTemplate(
		"generate_ddl_"+string(a.Kind())+"_bigquery.sql.tmpl",
		generateDDLData(t, a, r),
	)
}

// --- template data ---

type schemaReaderTmplData struct {
	SchemaReaderTable string
	Bucket            string
	WarehouseID       string
	SpaceID           string
	SchemaName        string
}

func schemaReaderData(t WarehouseTarget, a ArtifactType, r ValidationRun) schemaReaderTmplData {
	return schemaReaderTmplData{
		SchemaReaderTable: a.SchemaReaderTable(r.SpaceShortID()),
		Bucket:            t.Bucket(),
		WarehouseID:       r.WarehouseID,
		SpaceID:           r.SpaceID,
		SchemaName:        r.SchemaName,
	}
}

type generateDDLTmplData struct {
	LoaderTable       string
	SchemaReaderTable string
	Bucket            string
	WarehouseID       string
	SpaceID           string
	SchemaName        string
}

func generateDDLData(t WarehouseTarget, a ArtifactType, r ValidationRun) generateDDLTmplData {
	return generateDDLTmplData{
		LoaderTable:       a.LoaderTable(r.SpaceShortID()),
		SchemaReaderTable: a.SchemaReaderTable(r.SpaceShortID()),
		Bucket:            t.Bucket(),
		WarehouseID:       r.WarehouseID,
		SpaceID:           r.SpaceID,
		SchemaName:        r.SchemaName,
	}
}
