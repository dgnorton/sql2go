package main

import (
	"database/sql"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"strings"

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
	flag.Parse()

	db, err := sql.Open(*dbDriver, *dbConnect)
	check(err)
	defer db.Close()

	qry := fmt.Sprintf("SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES", *database)
	if *tables != "" {
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
		qry = fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'%s'", *database, tbl)
		rows, err := db.Query(qry)
		check(err)

		for rows.Next() {
			c := &Column{}
			check(rows.Scan(&c.Name, &c.Type))
			c.Type, err = goType(c.Type)
			check(err)
		}
		check(rows.Err())
	}

	check(genTablesCode(tbls))
}

func goType(t string) (string, error) {
	if t == "int" || t == "bigint" {
		return "int", nil
	} else if t == "bit" {
		return "bool", nil
	} else if strings.Contains(t, "char") {
		return "string", nil
	}
	return "", fmt.Errorf("don't know how to convert type: %s", t)
}

func genTablesCode(tables Tables) error {
	for _, t := range tables {
		if err := genTableCode(t); err != nil {
			return err
		}
	}
	return nil
}

func genTableCode(t *Table) error {
	return tableTemplate.Execute(os.Stdout, t)
}

var tableTemplate = template.Must(template.New("struct").Parse(tableTemplateText))

var tableTemplateText = `// {{ .Name }}Row represents one row from table {{ .Name }}
type {{ .Name }}Row struct {
	{{range .Columns}}
	{{.Name .Type}}
	{{ end }}
}`

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}