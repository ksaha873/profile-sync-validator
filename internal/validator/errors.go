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

// ValidationError indicates the final count(*) query returned a non-zero
// mismatch count — the archiver produced data that the loader did not write.
type ValidationError struct {
	Artifact      ArtifactKind
	QueryType     string // "inserts" | "deletes"
	MismatchCount int64
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation mismatch: %s %s has %d unmatched rows", e.Artifact, e.QueryType, e.MismatchCount)
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
