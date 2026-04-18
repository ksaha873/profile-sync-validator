package validator

import (
	"errors"
	"fmt"
	"strings"
)

// SystemError indicates the validation could not complete because of an
// infrastructure or programming failure (Athena down, DDL malformed, AWS auth,
// context cancelled, etc). These are retried by AthenaClient.
type SystemError struct {
	Step     string
	Attempts int
	Err      error
}

func (e *SystemError) Error() string {
	if e.Attempts > 0 {
		return fmt.Sprintf("system error at step %q after %d attempts: %v", e.Step, e.Attempts, e.Err)
	}
	return fmt.Sprintf("system error at step %q: %v", e.Step, e.Err)
}

func (e *SystemError) Unwrap() error { return e.Err }

// ValidationError indicates one of the 3 staged checks failed:
//   - Stage "expected_empty": 0 rows returned from the data_v3 side
//   - Stage "loader_empty":   0 rows returned from the loader side
//   - Stage "mismatch":       final left-join found unmatched rows
type ValidationError struct {
	Artifact      ArtifactKind
	QueryType     string // "inserts" | "deletes"
	Stage         string // "expected_empty" | "loader_empty" | "mismatch"
	Count         int64  // expected/loader row count for empty stages; mismatch count for mismatch stage
}

func (e *ValidationError) Error() string {
	switch e.Stage {
	case "expected_empty":
		return fmt.Sprintf("validation failure: %s %s expected-rows (data_v3) returned 0 rows", e.Artifact, e.QueryType)
	case "loader_empty":
		return fmt.Sprintf("validation failure: %s %s loader-rows returned 0 rows", e.Artifact, e.QueryType)
	default:
		return fmt.Sprintf("validation mismatch: %s %s has %d unmatched rows", e.Artifact, e.QueryType, e.Count)
	}
}

// MultiError aggregates errors from parallel steps. A non-nil MultiError always
// contains at least one error.
type MultiError []error

func (m MultiError) Error() string {
	parts := make([]string, 0, len(m))
	for _, e := range m {
		parts = append(parts, e.Error())
	}
	return strings.Join(parts, "; ")
}

func (m MultiError) Unwrap() []error { return []error(m) }

// joinErrors returns nil if all args are nil, a single error if exactly one is
// non-nil, or a MultiError otherwise.
func joinErrors(errs ...error) error {
	var out MultiError
	for _, e := range errs {
		if e != nil {
			out = append(out, e)
		}
	}
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	default:
		return out
	}
}

// IsSystemError reports whether err (or anything it wraps) is a *SystemError.
func IsSystemError(err error) bool {
	var s *SystemError
	return errors.As(err, &s)
}

// IsValidationError reports whether err (or anything it wraps) is a *ValidationError.
func IsValidationError(err error) bool {
	var v *ValidationError
	return errors.As(err, &v)
}
