package drysql

import (
	"fmt"
	"reflect"
	"strings"
	"database/sql"
	"database/sql/driver"
)

type SqlInterface interface {
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type DrySql struct {
	sqlImpl SqlInterface
}

func GetDrySqlImplementation(sqlImpl SqlInterface) DrySql {
	return DrySql{sqlImpl: sqlImpl}
}

func (drysql DrySql) PreparedExec(query string, inputs []interface{}) (sql.Result, error) {

	stmtOut, err := drysql.sqlImpl.Prepare(query)

	if err != nil {
		return nil, err
	}

	return stmtOut.Exec(inputs...)
}

func (drysql DrySql) ExecWithoutPrepare(query string) (result sql.Result, err error) {

	return drysql.sqlImpl.Exec(query)
}

func (drysql DrySql) QueryRow(query string, inputs []interface{}, outputs []interface{}) error {

	stmtOut, err := drysql.sqlImpl.Prepare(query)

	if err != nil {
		return err
	}

	row := stmtOut.QueryRow(inputs...)

	return row.Scan(outputs...)
}

func (drysql DrySql) PreparedQuery(query string, inputs []interface{}, scanner func(rows *sql.Rows) error) error {

	stmtOut, err := drysql.sqlImpl.Prepare(query)

	if err != nil {
		return err
	}

	var rows *sql.Rows
	if rows, err = stmtOut.Query(inputs...); err != nil {
		return err
	}

	if rows != nil {
		defer rows.Close()
	}

	for rows.Next() {
		if err = scanner(rows); err != nil {
			return err
		}
	}

	return rows.Err()
}

func (drysql DrySql) QueryWithoutPrepare(query string, scanner func(rows *sql.Rows) error) (err error) {

	var rows *sql.Rows
	if rows, err = drysql.sqlImpl.Query(query); err != nil {
		return err
	}

	if rows != nil {
		defer rows.Close()
	}

	for rows.Next() {
		if err = scanner(rows); err != nil {
			return err
		}
	}

	return rows.Err()
}


// Accepts a struct of optional pointers for updating mysql columns in the specified table
// All struct fields must include a db tag
// rowIdentifierTag identifies which struct field is the row key
// can include an optional fixed conditional params

func (drysql DrySql) UpdateTableRowFromStruct(tableName string, rowIdentifierTag string, updateStruct interface{}, optionalConditional string) (err error) {

	var columnsToUpdate string
	var inputs []interface{}
	var rowIdentifierValue interface{}
	t := reflect.TypeOf(updateStruct)
	v := reflect.ValueOf(updateStruct)

	// Iterate over all available fields and read the tag value
	for i := 0; i < t.NumField(); i++ {
		columnValue, err := driver.DefaultParameterConverter.ConvertValue(v.Field(i).Interface())
		if err != nil {
			return err
		}
		if columnValue != nil {
			// Get the field, returns https://golang.org/pkg/reflect/#StructField
			field := t.Field(i)
			columnKey := field.Tag.Get("db")
			if columnKey != "" {
				if strings.EqualFold(columnKey, rowIdentifierTag) {
					rowIdentifierValue = columnValue
				} else {
					if len(columnsToUpdate) != 0 {
						columnsToUpdate += ", "
					}
					columnsToUpdate += columnKey + " = ?"
					inputs = append(inputs, columnValue)
				}
			}
		}
	}

	if len(inputs) == 0{
		return nil
	}

	if len(optionalConditional) > 0{
		optionalConditional = " AND " + optionalConditional
	}

	inputs = append(inputs, rowIdentifierValue)

	query := "UPDATE " + tableName + " SET " + columnsToUpdate + " WHERE " + rowIdentifierTag + " = ?" + optionalConditional

	_, err = drysql.PreparedExec(query, inputs)

	return err
}
