package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	_ "github.com/denisenkom/go-mssqldb"
)

type Table struct {
	Name    string
	Columns Columns
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

	tbls := make(Tables, 0)
	for rows.Next() {
		t := &Table{}
		check(rows.Scan(&t.Name))
		tbls = append(tbls, t)
	}
	check(rows.Err())

	for _, tbl := range tbls {
		qry = fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'%s'", *database, tbl.Name)
		rows, err := db.Query(qry)
		check(err)

		for rows.Next() {
			c := &Column{}
			check(rows.Scan(&c.Name, &c.Type))
			c.Type, err = goType(c.Type, c.Name, tbl.Name)
			check(err)
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

	check(genTablesCode(w, *pkg, tbls))
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

func genTablesCode(w io.Writer, pkg string, tables Tables) error {
	check(fileHeaderTemplate.Execute(w, pkg))

	for _, t := range tables {
		if err := genTableCode(w, t); err != nil {
			return err
		}
	}
	return nil
}

func genTableCode(w io.Writer, t *Table) error {
	return tableTemplate.Execute(w, t)
}

var fileHeaderTemplate = template.Must(template.New("file").Parse(fileHeaderTemplateText))

var fileHeaderTemplateText = `// DO NOT EDIT!
// This file is MACHINE GENERATED

package {{ . }}

import (
	"database/sql"
)
`

var tableTemplate = template.Must(template.New("struct").Parse(tableTemplateText))

var tableTemplateText = `// {{ .Name }}Row represents one row from table {{ .Name }}.
type {{ .Name }}Row struct {
	{{range .Columns}}{{/*
		*/}}{{ .Name }} {{ .Type }}
	{{ end }}}

// scan{{ .Name }}Row scans and returns one {{ .Name }}Row.
func scan{{ .Name }}Row(rows *sql.Rows) (*{{ .Name }}Row, error) {
	r := &{{ .Name }}Row{}
	if err := rows.Scan({{range $i, $e := .Columns}}{{if $i}}, {{end}}&r.{{ $e.Name }}{{ end }}); err != nil {
		return nil, err
	}
	return r, nil
}

// {{ .Name }}Rows is an array of rows from table {{ .Name }}.
type {{ .Name }}Rows []*{{ .Name }}Row

// scan{{ .Name }}Rows scans all rows and retuns an array.
func scan{{ .Name }}Rows(rows *sql.Rows) ({{ .Name }}Rows, error) {
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

`

func check(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
