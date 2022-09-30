package drysql

import (
	"database/sql"
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
