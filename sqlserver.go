package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

type SQLServerSchemaReader struct {
	connStr      string
	exportFields bool
	dbInterface  bool
}

func NewSQLServerSchemaReader(connStr string, exportFields, dbInterface bool) *SQLServerSchemaReader {
	return &SQLServerSchemaReader{
		connStr:      connStr,
		exportFields: exportFields,
		dbInterface:  dbInterface,
	}
}

func (r *SQLServerSchemaReader) ReadTablesSchema(database, tables string) (Tables, error) {
	db, err := sql.Open("mssql", r.connStr)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	qry := fmt.Sprintf("SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES", database)
	if tables != "" {
		tables = fmt.Sprintf("'%s'", strings.Join(strings.Split(tables, ","), "','"))
		qry = fmt.Sprintf("%s WHERE TABLE_NAME IN (%s)", qry, tables)
	}

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

		cc, err := r.readColumnsSchema(db, database, t.Name)
		if err != nil {
			return nil, err
		}

		t.Columns = cc
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return tt, nil
}

func (r *SQLServerSchemaReader) readColumnsSchema(db *sql.DB, database, table string) (Columns, error) {
	qry := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'%s'", database, table)

	rows, err := db.Query(qry)
	if err != nil {
		return nil, err
	}

	cc := Columns{}
	for rows.Next() {
		c := &Column{}
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return nil, err
		}

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

func (r *SQLServerSchemaReader) goType(t, columnName, tableName string) (string, error) {
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
