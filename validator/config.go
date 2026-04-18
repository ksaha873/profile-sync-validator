package validator

import (
	"fmt"
	"strings"
	"time"
)

type ArtifactKind string

const (
	ArtifactIdentifiers ArtifactKind = "identifiers"
	ArtifactTraits      ArtifactKind = "traits"
)

type WarehouseKind string

const (
	WarehouseSnowflake WarehouseKind = "snowflake"
	WarehouseBigQuery  WarehouseKind = "bigquery"
)

type ValidationRun struct {
	SpaceID     string
	WarehouseID string
	SchemaName  string

	Day string

	StartAt string
	EndAt   string

	TombstonedStartAt string
	TombstonedEndAt   string

	Warehouse WarehouseKind

	// DataV3Table is the Athena table holding V3 patches (source of truth).
	// Defaults to "data_v3" if empty.
	DataV3Table string
}

func (r ValidationRun) Validate() error {
	if r.SpaceID == "" {
		return fmt.Errorf("space_id is required")
	}
	if !strings.HasPrefix(r.SpaceID, "spa_") {
		return fmt.Errorf("space_id must start with spa_: got %q", r.SpaceID)
	}
	if r.WarehouseID == "" {
		return fmt.Errorf("warehouse_id is required")
	}
	if r.SchemaName == "" {
		return fmt.Errorf("schema_name is required")
	}
	if _, err := time.Parse("2006-01-02", r.Day); err != nil {
		return fmt.Errorf("day must be YYYY-MM-DD: %w", err)
	}
	for name, v := range map[string]string{
		"start_at":            r.StartAt,
		"end_at":              r.EndAt,
		"tombstoned_start_at": r.TombstonedStartAt,
		"tombstoned_end_at":   r.TombstonedEndAt,
	} {
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			return fmt.Errorf("%s must be RFC3339: %w", name, err)
		}
	}
	switch r.Warehouse {
	case WarehouseSnowflake, WarehouseBigQuery:
	default:
		return fmt.Errorf("warehouse must be %q or %q: got %q", WarehouseSnowflake, WarehouseBigQuery, r.Warehouse)
	}
	return nil
}

// SpaceShortID returns the space_id as-is, used for suffixing table names.
// Athena table identifiers accept the full spa_<base62> form, so no transformation is needed.
func (r ValidationRun) SpaceShortID() string {
	return r.SpaceID
}

func (r ValidationRun) dataV3Table() string {
	if r.DataV3Table == "" {
		return "data_v3"
	}
	return r.DataV3Table
}
