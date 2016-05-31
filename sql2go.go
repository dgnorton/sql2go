package main

import (
	"flag"
	"fmt"
	"os"
	"text/template"
	"unicode"
)

type TemplateData struct {
	Package     string
	Imports     []string
	DBInterface string
	Tables      Tables
}

type Table struct {
	Name    string
	Columns Columns
	RowType string
}

type Tables []*Table

type Column struct {
	Name string
	Type string
}

type Columns []*Column

type SchemaReader interface {
	ReadTablesSchema(database, tables string) (Tables, error)
}

func main() {
	dbDriver := flag.String("driver", "mssql", "database driver")
	dbConnect := flag.String("dbconnect", "", "database connect string")
	database := flag.String("database", "", "database name")
	tables := flag.String("tables", "", "comma delimited list of tables")
	pkg := flag.String("pkg", "main", "package the generated code will be part of")
	outfile := flag.String("outfile", "", "output file")
	exportFields := flag.Bool("exportfields", true, "upper-case first letter of table column names in generated code")
	dbinterface := flag.Bool("dbinterface", false, "generates a DB interface useful for mock tests")
	flag.Parse()

	td := TemplateData{
		Package: *pkg,
		Imports: []string{"time"},
	}

	if *dbinterface {
		td.DBInterface = dbInterfaceCode
	} else {
		td.Imports = append(td.Imports, "database/sql")
	}

	var r SchemaReader
	switch *dbDriver {
	case "mssql":
		r = NewSQLServerSchemaReader(*dbConnect, *exportFields, *dbinterface)
	case "mysql":
		r = NewMySQLSchemaReader(*dbConnect, *exportFields, *dbinterface)
	default:
		check(fmt.Errorf("unsupported DB driver: %s", *dbDriver))
	}

	tt, err := r.ReadTablesSchema(*database, *tables)
	check(err)
	td.Tables = tt

	w := os.Stdout
	if *outfile != "" {
		w, err = os.OpenFile(*outfile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		check(err)
		defer w.Close()
	}

	check(codeTemplate.Execute(w, td))
}

var codeTemplate = template.Must(template.New("code").Parse(codeTemplateText))

var codeTemplateText = `// DO NOT EDIT!
// This file is MACHINE GENERATED

package {{ .Package }}

import (
	{{range .Imports}}{{/*
		*/}}"{{ . }}"
	{{ end }}
)

{{ .DBInterface }}

{{range .Tables}}
// {{ .Name }}Row represents one row from table {{ .Name }}.
type {{ .Name }}Row struct {
	{{range .Columns}}{{/*
		*/}}{{ .Name }} {{ .Type }}
	{{ end }}}

// scan{{ .Name }}Row scans and returns one {{ .Name }}Row.
func scan{{ .Name }}Row(rows {{ .RowType }}) (*{{ .Name }}Row, error) {
	r := &{{ .Name }}Row{}
	if err := rows.Scan({{range $i, $e := .Columns}}{{if $i}}, {{end}}&r.{{ $e.Name }}{{ end }}); err != nil {
		return nil, err
	}
	return r, nil
}

// {{ .Name }}Rows is an array of rows from table {{ .Name }}.
type {{ .Name }}Rows []*{{ .Name }}Row

// scan{{ .Name }}Rows scans all rows and retuns an array.
func scan{{ .Name }}Rows(rows {{ .RowType }}) ({{ .Name }}Rows, error) {
	rs := make({{ .Name }}Rows, 0)
	for rows.Next() {
		row, err := scan{{ .Name }}Row(rows)
		if err != nil {
			return nil, err
		}
		rs = append(rs, row)
	}
	return rs, nil
}
{{ end }}
`

var dbInterfaceCode = `// DB is an interface to a database.
type DB interface {
	Close() error
	Query(query string, args ...interface{}) (Rows, error)
}

type Row interface {
	Scan(dest ...interface{}) error
}

type Rows interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(dest ...interface{}) error
}`

func toUpperFirstChar(s string) string {
	a := []rune(s)
	a[0] = unicode.ToUpper(a[0])
	return string(a)
}

func check(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
