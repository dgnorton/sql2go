package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLSchemaReader struct {
	connStr      string
	exportFields bool
	dbInterface  bool
}

func NewMySQLSchemaReader(connStr string, exportFields, dbInterface bool) *MySQLSchemaReader {
	return &MySQLSchemaReader{
		connStr:      connStr,
		exportFields: exportFields,
		dbInterface:  dbInterface,
	}
}

func (r *MySQLSchemaReader) ReadTablesSchema(database, tables string) (Tables, error) {
	db, err := sql.Open("mysql", r.connStr)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// If specific tables were requested, put those table names in a set.
	var tmap map[string]struct{}
	if tables != "" {
		tmap := make(map[string]struct{})
		names := strings.Split(tables, ",")
		for _, name := range names {
			tmap[name] = struct{}{}
		}
	}

	qry := fmt.Sprintf("SHOW TABLES")
	rows, err := db.Query(qry)
	if err != nil {
		return nil, err
	}

	rowType := "*sql.Rows"
	if r.dbInterface {
		rowType = "Rows"
	}

	tt := Tables{}
	for rows.Next() {
		t := &Table{RowType: rowType}
		if err := rows.Scan(&t.Name); err != nil {
			return nil, err
		}

		if tmap != nil {
			// See if table is one of the set we're looking for.
			if _, ok := tmap[t.Name]; !ok {
				continue
			}
		}

		cc, err := r.readColumnsSchema(db, database, t.Name)
		if err != nil {
			return nil, err
		}

		t.Columns = cc

		tt = append(tt, t)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	spew.Dump(tt)
	return tt, nil
}

func (r *MySQLSchemaReader) readColumnsSchema(db *sql.DB, database, table string) (Columns, error) {
	qry := fmt.Sprintf("DESCRIBE %s", table)

	rows, err := db.Query(qry)
	if err != nil {
		return nil, err
	}

	col := struct {
		field string
		typ   string
		null  string
		key   string
		def   *string
		extra string
	}{}

	cc := Columns{}
	for rows.Next() {
		if err := rows.Scan(&col.field, &col.typ, &col.null, &col.key, &col.def, &col.extra); err != nil {
			return nil, err
		}

		c := &Column{Name: col.field, Type: col.typ}
		t, err := r.goType(c.Type, c.Name, table)
		if err != nil {
			return nil, err
		}
		c.Type = t

		if r.exportFields {
			c.Name = toUpperFirstChar(c.Name)
		}

		cc = append(cc, c)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return cc, nil
}

func (r *MySQLSchemaReader) goType(t, columnName, tableName string) (string, error) {
	if strings.Contains(t, "(") {
		t = strings.Split(t, "(")[0]
	}
	switch t {
	case "int", "bigint":
		return "int", nil
	case "bit":
		return "bool", nil
	case "char", "nchar", "varchar", "nvarchar", "text":
		return "string", nil
	case "datetime", "date":
		return "time.Time", nil
	default:
		return "", fmt.Errorf("don't know how to convert type: %s [%s.%s]", t, tableName, columnName)
	}
}
