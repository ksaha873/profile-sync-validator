package validator

import (
	"context"
	"fmt"
	"log"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Orchestrator runs the full validation pipeline for a single artifact type
// (identifiers OR traits) against a single warehouse target. Spawn one per
// (artifact, warehouse) combination and fan them out at the call site.
type Orchestrator struct {
	athena   *AthenaClient
	run      ValidationRun
	target   WarehouseTarget
	artifact ArtifactType

	mu            sync.Mutex
	createdTables []string
}

func NewOrchestrator(a *AthenaClient, r ValidationRun, target WarehouseTarget, artifact ArtifactType) *Orchestrator {
	return &Orchestrator{
		athena:   a,
		run:      r,
		target:   target,
		artifact: artifact,
	}
}

// Run executes the pipeline:
//  1. create schema_reader table (DROP + CREATE, sequential)
//  2. create loader table (DROP + generate DDL + exec DDL, sequential)
//  3. run inserts and deletes in parallel; collect both errors
//
// All created tables are dropped via deferred cleanup regardless of outcome.
func (o *Orchestrator) Run(ctx context.Context) error {
	defer o.dropAllTables()

	if err := o.createSchemaReader(ctx); err != nil {
		return err
	}
	if err := o.createLoaderTable(ctx); err != nil {
		return err
	}

	var eg errgroup.Group
	var insertsErr, deletesErr error
	eg.Go(func() error {
		insertsErr = o.runValidation(ctx, "inserts", o.artifact.InsertsSQL)
		return nil
	})
	eg.Go(func() error {
		deletesErr = o.runDeletesWithTombstoneCheck(ctx)
		return nil
	})
	_ = eg.Wait()
	return joinErrors(insertsErr, deletesErr)
}

// runDeletesWithTombstoneCheck runs the deletes validation (which returns 4
// counts: expected, loader, mismatch, reverse_mismatch) and runs the
// tombstone-exclusion follow-up if mismatch > 0. Returns a MultiError with both
// the post-tombstone mismatch and the reverse mismatch when both are non-zero;
// either can be nil independently.
func (o *Orchestrator) runDeletesWithTombstoneCheck(ctx context.Context) error {
	step := fmt.Sprintf("run_deletes_%s", o.artifact.Kind())
	sql, err := o.artifact.DeletesSQL(o.target, o.run)
	if err != nil {
		return &SystemError{Step: step + "_render", Err: err}
	}
	counts, err := o.athena.ExecRowInt64s(ctx, step, sql)
	if err != nil {
		return err
	}
	if len(counts) != 4 {
		return &SystemError{Step: step, Err: fmt.Errorf("expected 4 counts, got %d", len(counts))}
	}
	expected, loader, mismatch, reverse := counts[0], counts[1], counts[2], counts[3]
	log.Printf("[%s deletes] expected=%d loader=%d mismatch=%d reverse_mismatch=%d",
		o.artifact.Kind(), expected, loader, mismatch, reverse)

	switch {
	case expected == 0:
		return &ValidationError{Artifact: o.artifact.Kind(), QueryType: "deletes", Stage: "expected_empty"}
	case loader == 0:
		return &ValidationError{Artifact: o.artifact.Kind(), QueryType: "deletes", Stage: "loader_empty"}
	}

	var primaryErr, reverseErr error
	if mismatch > 0 {
		tsStep := fmt.Sprintf("run_tombstoned_check_deletes_%s", o.artifact.Kind())
		tsSQL, rerr := o.artifact.TombstonedCheckSQL(o.target, o.run)
		if rerr != nil {
			return &SystemError{Step: tsStep + "_render", Err: rerr}
		}
		remaining, cerr := o.athena.ExecScalarInt64(ctx, tsStep, tsSQL)
		if cerr != nil {
			return cerr
		}
		log.Printf("[%s deletes] tombstone-excluded remaining=%d (was mismatch=%d)", o.artifact.Kind(), remaining, mismatch)
		if remaining > 0 {
			primaryErr = &ValidationError{
				Artifact: o.artifact.Kind(), QueryType: "deletes", Stage: "mismatch", Count: remaining,
			}
		}
	}
	if reverse > 0 {
		reverseErr = &ValidationError{
			Artifact: o.artifact.Kind(), QueryType: "deletes", Stage: "reverse_mismatch", Count: reverse,
		}
	}
	return joinErrors(primaryErr, reverseErr)
}

func (o *Orchestrator) createSchemaReader(ctx context.Context) error {
	step := fmt.Sprintf("create_schema_reader_%s", o.artifact.Kind())
	tbl := o.artifact.SchemaReaderTable(o.run.SpaceShortID())

	if _, err := o.athena.Exec(ctx, step+"_drop", "DROP TABLE IF EXISTS "+tbl); err != nil {
		return err
	}

	sql, err := o.target.SchemaReaderSQL(o.artifact, o.run)
	if err != nil {
		return &SystemError{Step: step, Err: err}
	}
	if _, err := o.athena.Exec(ctx, step, sql); err != nil {
		return err
	}
	o.track(tbl)
	return nil
}

func (o *Orchestrator) createLoaderTable(ctx context.Context) error {
	step := fmt.Sprintf("create_loader_%s", o.artifact.Kind())
	tbl := o.artifact.LoaderTable(o.run.SpaceShortID())

	if _, err := o.athena.Exec(ctx, step+"_drop", "DROP TABLE IF EXISTS "+tbl); err != nil {
		return err
	}

	genSQL, err := o.target.GenerateDDLSQL(o.artifact, o.run)
	if err != nil {
		return &SystemError{Step: step + "_render_meta", Err: err}
	}
	ddl, err := o.athena.ExecScalarString(ctx, step+"_generate", genSQL)
	if err != nil {
		return err
	}
	if _, err := o.athena.Exec(ctx, step+"_exec", ddl); err != nil {
		return err
	}
	o.track(tbl)
	return nil
}

type sqlRenderer func(t WarehouseTarget, r ValidationRun) (string, error)

func (o *Orchestrator) runValidation(ctx context.Context, queryType string, render sqlRenderer) error {
	step := fmt.Sprintf("run_%s_%s", queryType, o.artifact.Kind())
	sql, err := render(o.target, o.run)
	if err != nil {
		return &SystemError{Step: step + "_render", Err: err}
	}
	counts, err := o.athena.ExecRowInt64s(ctx, step, sql)
	if err != nil {
		return err
	}
	if len(counts) != 3 {
		return &SystemError{Step: step, Err: fmt.Errorf("expected 3 counts, got %d", len(counts))}
	}
	expected, loader, mismatch := counts[0], counts[1], counts[2]
	log.Printf("[%s %s] expected=%d loader=%d mismatch=%d", o.artifact.Kind(), queryType, expected, loader, mismatch)

	switch {
	case expected == 0:
		return &ValidationError{Artifact: o.artifact.Kind(), QueryType: queryType, Stage: "expected_empty", Count: 0}
	case loader == 0:
		return &ValidationError{Artifact: o.artifact.Kind(), QueryType: queryType, Stage: "loader_empty", Count: 0}
	case mismatch > 0:
		return &ValidationError{Artifact: o.artifact.Kind(), QueryType: queryType, Stage: "mismatch", Count: mismatch}
	}
	return nil
}

func (o *Orchestrator) track(tbl string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.createdTables = append(o.createdTables, tbl)
}

// dropAllTables runs on a fresh background context so cleanup proceeds even
// when the parent ctx is cancelled. Drop failures are logged, not returned —
// orphaned external tables are cheap compared to masking the primary error.
func (o *Orchestrator) dropAllTables() {
	o.mu.Lock()
	tables := append([]string(nil), o.createdTables...)
	o.mu.Unlock()

	ctx := context.Background()
	for _, tbl := range tables {
		if _, err := o.athena.Exec(ctx, "cleanup_drop", "DROP TABLE IF EXISTS "+tbl); err != nil {
			log.Printf("cleanup: failed to drop %s: %v", tbl, err)
		}
	}
}
