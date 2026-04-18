package validator

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

const (
	maxAttempts   = 3
	pollInterval  = 2 * time.Second
	queryPollWait = 30 * time.Minute
)

var retryBackoff = []time.Duration{
	1 * time.Second,
	4 * time.Second,
	16 * time.Second,
}

// AthenaClient is a thin wrapper around the Athena SDK that executes single
// statements and retries SystemErrors with exponential backoff. It has no
// knowledge of the validation queries' shape.
type AthenaClient struct {
	api        AthenaAPI
	database   string
	workgroup  string
	outputLoc  string
	catalog    string
	sleeper    func(time.Duration) // for tests
}

// AthenaAPI is the subset of the athena service client used here.
type AthenaAPI interface {
	StartQueryExecution(ctx context.Context, in *athena.StartQueryExecutionInput, opts ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
	GetQueryExecution(ctx context.Context, in *athena.GetQueryExecutionInput, opts ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
	GetQueryResults(ctx context.Context, in *athena.GetQueryResultsInput, opts ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
}

type AthenaConfig struct {
	Database  string
	Workgroup string
	OutputLoc string // s3://bucket/prefix/ for query results
	Catalog   string // e.g. "AwsDataCatalog"; optional
}

func NewAthenaClient(api AthenaAPI, cfg AthenaConfig) *AthenaClient {
	if cfg.Catalog == "" {
		cfg.Catalog = "AwsDataCatalog"
	}
	return &AthenaClient{
		api:       api,
		database:  cfg.Database,
		workgroup: cfg.Workgroup,
		outputLoc: cfg.OutputLoc,
		catalog:   cfg.Catalog,
		sleeper:   time.Sleep,
	}
}

// Exec runs a single SQL statement and waits for completion. Returns the
// QueryExecutionId on success.
func (c *AthenaClient) Exec(ctx context.Context, step, sql string) (string, error) {
	return c.execWithRetry(ctx, step, sql)
}

// ExecScalarString runs a single SQL statement and returns the VarCharValue of
// the first column of the first data row. Used to read the DDL string from the
// generate-DDL meta query.
func (c *AthenaClient) ExecScalarString(ctx context.Context, step, sql string) (string, error) {
	qid, err := c.execWithRetry(ctx, step, sql)
	if err != nil {
		return "", err
	}
	out, err := c.api.GetQueryResults(ctx, &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(qid),
		MaxResults:       aws.Int32(2), // header + first row
	})
	if err != nil {
		return "", &SystemError{Step: step, Err: fmt.Errorf("get_query_results: %w", err)}
	}
	if len(out.ResultSet.Rows) < 2 {
		return "", &SystemError{Step: step, Err: fmt.Errorf("expected at least one data row, got %d", len(out.ResultSet.Rows))}
	}
	row := out.ResultSet.Rows[1]
	if len(row.Data) == 0 || row.Data[0].VarCharValue == nil {
		return "", &SystemError{Step: step, Err: fmt.Errorf("first cell is empty")}
	}
	return *row.Data[0].VarCharValue, nil
}

// ExecScalarInt64 runs a single SQL statement and returns the integer value of
// the first column of the first data row. Used for COUNT(*) validation queries.
func (c *AthenaClient) ExecScalarInt64(ctx context.Context, step, sql string) (int64, error) {
	s, err := c.ExecScalarString(ctx, step, sql)
	if err != nil {
		return 0, err
	}
	var n int64
	if _, err := fmt.Sscan(s, &n); err != nil {
		return 0, &SystemError{Step: step, Err: fmt.Errorf("parse int from %q: %w", s, err)}
	}
	return n, nil
}

// ExecRowInt64s runs a single SQL statement and returns the integer values of
// every column in the first data row. Used for multi-count validation queries
// that return expected / loader / mismatch counts in one shot.
func (c *AthenaClient) ExecRowInt64s(ctx context.Context, step, sql string) ([]int64, error) {
	qid, err := c.execWithRetry(ctx, step, sql)
	if err != nil {
		return nil, err
	}
	out, err := c.api.GetQueryResults(ctx, &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(qid),
		MaxResults:       aws.Int32(2),
	})
	if err != nil {
		return nil, &SystemError{Step: step, Err: fmt.Errorf("get_query_results: %w", err)}
	}
	if len(out.ResultSet.Rows) < 2 {
		return nil, &SystemError{Step: step, Err: fmt.Errorf("expected at least one data row, got %d", len(out.ResultSet.Rows))}
	}
	row := out.ResultSet.Rows[1]
	vals := make([]int64, 0, len(row.Data))
	for i, cell := range row.Data {
		if cell.VarCharValue == nil {
			return nil, &SystemError{Step: step, Err: fmt.Errorf("column %d is empty", i)}
		}
		var n int64
		if _, err := fmt.Sscan(*cell.VarCharValue, &n); err != nil {
			return nil, &SystemError{Step: step, Err: fmt.Errorf("parse int from column %d %q: %w", i, *cell.VarCharValue, err)}
		}
		vals = append(vals, n)
	}
	return vals, nil
}

func (c *AthenaClient) execWithRetry(ctx context.Context, step, sql string) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		qid, err := c.execOnce(ctx, step, sql)
		if err == nil {
			return qid, nil
		}
		lastErr = err
		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return "", &SystemError{Step: step, Attempts: attempt, Err: ctx.Err()}
			default:
			}
			c.sleeper(retryBackoff[attempt-1])
		}
	}
	return "", &SystemError{Step: step, Attempts: maxAttempts, Err: lastErr}
}

func (c *AthenaClient) execOnce(ctx context.Context, step, sql string) (string, error) {
	start, err := c.api.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Database: aws.String(c.database),
			Catalog:  aws.String(c.catalog),
		},
		WorkGroup: aws.String(c.workgroup),
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String(c.outputLoc),
		},
	})
	if err != nil {
		return "", fmt.Errorf("start_query_execution: %w", err)
	}
	qid := aws.ToString(start.QueryExecutionId)

	deadline := time.Now().Add(queryPollWait)
	for {
		select {
		case <-ctx.Done():
			return qid, ctx.Err()
		default:
		}
		st, err := c.api.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(qid),
		})
		if err != nil {
			return qid, fmt.Errorf("get_query_execution: %w", err)
		}
		status := st.QueryExecution.Status
		switch status.State {
		case athenatypes.QueryExecutionStateSucceeded:
			return qid, nil
		case athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled:
			reason := aws.ToString(status.StateChangeReason)
			return qid, fmt.Errorf("query %s: %s", status.State, reason)
		}
		if time.Now().After(deadline) {
			return qid, fmt.Errorf("query %s exceeded poll deadline", qid)
		}
		c.sleeper(pollInterval)
	}
}
