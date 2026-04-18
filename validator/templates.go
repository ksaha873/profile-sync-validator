package validator

import (
	"bytes"
	"embed"
	"fmt"
	"text/template"
)

//go:embed sql/*.sql.tmpl
var sqlFS embed.FS

var templates = template.Must(template.ParseFS(sqlFS, "sql/*.sql.tmpl"))

// renderTemplate renders the named template (filename without directory) with the given data.
func renderTemplate(name string, data any) (string, error) {
	var buf bytes.Buffer
	if err := templates.ExecuteTemplate(&buf, name, data); err != nil {
		return "", fmt.Errorf("render %s: %w", name, err)
	}
	return buf.String(), nil
}
