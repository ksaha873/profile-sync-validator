package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"golang.org/x/sync/errgroup"

	"github.com/ksaha873/profile-sync-validator/internal/validator"
)

func main() {
	spaceID := flag.String("space-id", "", "Personas space ID, e.g. spa_7dwJTm2tmQFEwcECEbYMu3 (required)")
	warehouseID := flag.String("warehouse-id", "", "Warehouse source ID, e.g. w5tevAWoyfNRmdisUcdavB (required)")
	schemaName := flag.String("schema-name", "", "Project slug / schema name, e.g. personas_main_2 (required)")
	day := flag.String("day", "", "Day to validate YYYY-MM-DD (required)")
	startAt := flag.String("start-at", "", "Window start RFC3339 (required)")
	endAt := flag.String("end-at", "", "Window end RFC3339 (required)")
	tombStart := flag.String("tombstoned-start-at", "", "Tombstoned window start RFC3339, should be >=2h before start-at (required)")
	tombEnd := flag.String("tombstoned-end-at", "", "Tombstoned window end RFC3339, should be >=2h after end-at (required)")
	warehouse := flag.String("warehouse", "", "snowflake | bigquery (required)")
	artifacts := flag.String("artifacts", "identifiers,traits", "Comma-separated: identifiers,traits")

	athenaDB := flag.String("athena-db", "patch_v3", "Athena database")
	athenaWG := flag.String("athena-workgroup", "primary", "Athena workgroup")
	athenaOutput := flag.String("athena-output", "", "Athena query result S3 location, e.g. s3://aws-athena-query-results-xxx/ (required)")
	dataV3Table := flag.String("data-v3-table", "data_v3", "Athena table holding V3 patches")

	flag.Parse()

	run := validator.ValidationRun{
		SpaceID:           *spaceID,
		WarehouseID:       *warehouseID,
		SchemaName:        *schemaName,
		Day:               *day,
		StartAt:           *startAt,
		EndAt:             *endAt,
		TombstonedStartAt: *tombStart,
		TombstonedEndAt:   *tombEnd,
		Warehouse:         validator.WarehouseKind(strings.ToLower(*warehouse)),
		DataV3Table:       *dataV3Table,
	}
	if err := run.Validate(); err != nil {
		fail(err)
	}
	if *athenaOutput == "" {
		fail(fmt.Errorf("athena-output is required"))
	}

	kinds, err := parseArtifacts(*artifacts)
	if err != nil {
		fail(err)
	}

	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fail(fmt.Errorf("load aws config: %w", err))
	}
	athenaClient := validator.NewAthenaClient(
		athena.NewFromConfig(awsCfg),
		validator.AthenaConfig{
			Database:  *athenaDB,
			Workgroup: *athenaWG,
			OutputLoc: *athenaOutput,
		},
	)
	target, err := validator.NewWarehouseTarget(run.Warehouse)
	if err != nil {
		fail(err)
	}

	var eg errgroup.Group
	errs := make([]error, len(kinds))
	for i, k := range kinds {
		i, k := i, k
		eg.Go(func() error {
			artifact, err := validator.NewArtifactType(k)
			if err != nil {
				errs[i] = err
				return nil
			}
			errs[i] = validator.NewOrchestrator(athenaClient, run, target, artifact).Run(ctx)
			return nil
		})
	}
	_ = eg.Wait()

	var sysErrs, valErrs []error
	for _, e := range errs {
		if e == nil {
			continue
		}
		if validator.IsValidationError(e) {
			valErrs = append(valErrs, e)
		} else {
			sysErrs = append(sysErrs, e)
		}
	}

	report(kinds, errs)

	switch {
	case len(sysErrs) > 0:
		os.Exit(2)
	case len(valErrs) > 0:
		os.Exit(1)
	default:
		os.Exit(0)
	}
}

func parseArtifacts(s string) ([]validator.ArtifactKind, error) {
	parts := strings.Split(s, ",")
	out := make([]validator.ArtifactKind, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.ToLower(p))
		switch p {
		case string(validator.ArtifactIdentifiers):
			out = append(out, validator.ArtifactIdentifiers)
		case string(validator.ArtifactTraits):
			out = append(out, validator.ArtifactTraits)
		case "":
		default:
			return nil, fmt.Errorf("unknown artifact %q", p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one artifact required")
	}
	return out, nil
}

func report(kinds []validator.ArtifactKind, errs []error) {
	for i, k := range kinds {
		switch {
		case errs[i] == nil:
			fmt.Printf("[%s] OK: 0 mismatches\n", k)
		case validator.IsValidationError(errs[i]):
			var me validator.MultiError
			if errors.As(errs[i], &me) {
				for _, e := range me {
					fmt.Printf("[%s] MISMATCH: %v\n", k, e)
				}
			} else {
				fmt.Printf("[%s] MISMATCH: %v\n", k, errs[i])
			}
		default:
			fmt.Printf("[%s] SYSTEM ERROR: %v\n", k, errs[i])
		}
	}
}

func fail(err error) {
	log.Printf("fatal: %v", err)
	flag.Usage()
	os.Exit(2)
}
