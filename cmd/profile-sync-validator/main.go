package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/aws/aws-sdk-go-v2/aws"
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

	athenaDB := flag.String("athena-db", "", "Athena database (required)")
	athenaWG := flag.String("athena-workgroup", "primary", "Athena workgroup; its configured result S3 location is used as the query output")
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
	if *athenaDB == "" {
		fail(fmt.Errorf("athena-db is required"))
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
	athenaAPI := athena.NewFromConfig(awsCfg)
	outputLoc, err := resolveWorkgroupOutput(ctx, athenaAPI, *athenaWG)
	if err != nil {
		fail(err)
	}
	athenaClient := validator.NewAthenaClient(
		athenaAPI,
		validator.AthenaConfig{
			Database:  *athenaDB,
			Workgroup: *athenaWG,
			OutputLoc: outputLoc,
		},
	)
	target, err := validator.NewWarehouseTarget(run.Warehouse)
	if err != nil {
		fail(err)
	}

	var eg errgroup.Group
	errs := make([]error, len(kinds))
	results := make([][]validator.StepResult, len(kinds))
	for i, k := range kinds {
		i, k := i, k
		eg.Go(func() error {
			artifact, err := validator.NewArtifactType(k)
			if err != nil {
				errs[i] = err
				return nil
			}
			res, runErr := validator.NewOrchestrator(athenaClient, run, target, artifact).Run(ctx)
			results[i] = res
			errs[i] = runErr
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
	printSummaryTable(results)

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

func printSummaryTable(results [][]validator.StepResult) {
	var rows []validator.StepResult
	for _, steps := range results {
		for _, s := range steps {
			if s.Artifact == "" {
				continue
			}
			rows = append(rows, s)
		}
	}
	if len(rows) == 0 {
		return
	}

	fmt.Println()
	fmt.Println("Summary:")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ARTIFACT\tOPERATION\tEXPECTED\tLOADER\tMISMATCH (RAW)\tMISMATCH (POST-TOMBSTONE)\tREVERSE MISMATCH\tSTATUS")
	for _, r := range rows {
		post := "-"
		if r.QueryType == "deletes" {
			if r.TombstoneChecked {
				post = fmt.Sprintf("%d", r.Mismatch)
			} else {
				post = "n/a"
			}
		}
		reverse := "-"
		if r.QueryType == "deletes" {
			reverse = fmt.Sprintf("%d", r.ReverseMismatch)
		}
		status := rowStatus(r)
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\n",
			r.Artifact, r.QueryType, r.Expected, r.Loader, r.MismatchRaw, post, reverse, status)
	}
	tw.Flush()
}

func rowStatus(r validator.StepResult) string {
	effective := r.MismatchRaw
	if r.QueryType == "deletes" && r.TombstoneChecked {
		effective = r.Mismatch
	}
	if effective == 0 && r.ReverseMismatch == 0 && r.Expected > 0 && r.Loader > 0 {
		return "PASS"
	}
	return "FAIL"
}

func resolveWorkgroupOutput(ctx context.Context, api *athena.Client, workgroup string) (string, error) {
	out, err := api.GetWorkGroup(ctx, &athena.GetWorkGroupInput{WorkGroup: aws.String(workgroup)})
	if err != nil {
		return "", fmt.Errorf("get workgroup %q: %w", workgroup, err)
	}
	if out.WorkGroup == nil || out.WorkGroup.Configuration == nil || out.WorkGroup.Configuration.ResultConfiguration == nil {
		return "", fmt.Errorf("workgroup %q has no result configuration", workgroup)
	}
	loc := aws.ToString(out.WorkGroup.Configuration.ResultConfiguration.OutputLocation)
	if loc == "" {
		return "", fmt.Errorf("workgroup %q has no result OutputLocation set", workgroup)
	}
	return loc, nil
}

func fail(err error) {
	log.Printf("fatal: %v", err)
	flag.Usage()
	os.Exit(2)
}
