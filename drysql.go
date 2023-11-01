package drysql

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
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

type SqlLoggingInterface interface {
	AddSqlRead()
	AddSqlWrite()
}

var SqlLogger SqlLoggingInterface

func (drysql DrySql) PreparedExec(query string, inputs []interface{}) (sql.Result, error) {

	stmtOut, err := drysql.sqlImpl.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmtOut.Close()

	if SqlLogger != nil {
		SqlLogger.AddSqlWrite()
	}

	return stmtOut.Exec(inputs...)
}

func (drysql DrySql) ExecWithoutPrepare(query string, args ...interface{}) (result sql.Result, err error) {

	return drysql.sqlImpl.Exec(query, args)
}

func (drysql DrySql) QueryRow(query string, inputs []interface{}, outputs []interface{}) error {

	stmtOut, err := drysql.sqlImpl.Prepare(query)
	if err != nil {
		return err
	}
	defer stmtOut.Close()

	if SqlLogger != nil {
		SqlLogger.AddSqlRead()
	}

	row := stmtOut.QueryRow(inputs...)

	return row.Scan(outputs...)
}

func (drysql DrySql) PreparedQuery(query string, inputs []interface{}, scanner func(rows *sql.Rows) error) error {

	stmtOut, err := drysql.sqlImpl.Prepare(query)
	if err != nil {
		return err
	}
	defer stmtOut.Close()

	if SqlLogger != nil {
		SqlLogger.AddSqlRead()
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

// UpdateTableRowFromStruct

// Accepts a struct of optional pointers for updating mysql columns in the specified table
// Use the `db:"column_name"` to tag struct fields with column name.  All struct fields must include a db tag
// rowIdentifierTag identifies which struct field is the row key
// Only the non-nil values from tagged fields in the struct will be updated.
// can include an optional fixed conditional params

/* 	EXAMPLE USAGE

	userUpdate := struct{
		UserID `db:"user_id"`
		FirstName *string `db:"first_name"`
		LastName *string `db:"last_name"`
	}

	err = drysql.UpdateTableRowFromStruct("my_users", "user_id", userUpdate)
*/

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

	if len(inputs) == 0 {
		return nil
	}

	if len(optionalConditional) > 0 {
		optionalConditional = " AND " + optionalConditional
	}

	inputs = append(inputs, rowIdentifierValue)

	query := "UPDATE " + tableName + " SET " + columnsToUpdate + " WHERE " + rowIdentifierTag + " = ?" + optionalConditional

	// don't use a prepared statement as reuse is less likely with these dynamic queries
	_, err = drysql.PreparedExec(query, inputs)

	return err
}
