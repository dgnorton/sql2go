package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/template"
	"unicode"

	_ "github.com/denisenkom/go-mssqldb"
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

	db, err := sql.Open(*dbDriver, *dbConnect)
	check(err)
	defer db.Close()

	qry := fmt.Sprintf("SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES", *database)
	if *tables != "" {
		*tables = fmt.Sprintf("'%s'", strings.Join(strings.Split(*tables, ","), "','"))
		qry = fmt.Sprintf("%s WHERE TABLE_NAME IN (%s)", qry, *tables)
	}
	rows, err := db.Query(qry)
	check(err)

	td := TemplateData{
		Package: *pkg,
		Imports: []string{"time"},
	}

	rowType := "*sql.Rows"
	if *dbinterface {
		rowType = "Rows"
		td.DBInterface = dbInterfaceCode
	} else {
		td.Imports = append(td.Imports, "database/sql")
	}

	for rows.Next() {
		t := &Table{RowType: rowType}
		check(rows.Scan(&t.Name))
		td.Tables = append(td.Tables, t)
	}
	check(rows.Err())

	for _, tbl := range td.Tables {
		qry = fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'%s'", *database, tbl.Name)
		rows, err := db.Query(qry)
		check(err)

		for rows.Next() {
			c := &Column{}
			check(rows.Scan(&c.Name, &c.Type))
			c.Type, err = goType(c.Type, c.Name, tbl.Name)
			check(err)
			if *exportFields {
				c.Name = toUpperFirstChar(c.Name)
			}
			tbl.Columns = append(tbl.Columns, c)
		}
		check(rows.Err())
	}

	w := os.Stdout
	if *outfile != "" {
		w, err = os.OpenFile(*outfile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		check(err)
		defer w.Close()
	}

	check(codeTemplate.Execute(w, td))
}

func goType(t, columnName, tableName string) (string, error) {
	switch t {
	case "int", "bigint":
		return "int", nil
	case "bit":
		return "bool", nil
	case "char", "nchar", "varchar", "nvarchar":
		return "string", nil
	case "datetime":
		return "time.Time", nil
	default:
		return "", fmt.Errorf("don't know how to convert type: %s [%s.%s]", t, tableName, columnName)
	}
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
