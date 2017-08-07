package main

import (
	"database/sql"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// 2.6
func (t *Terminal) OrderStatus(db *sql.DB) error {
	// TODO: implement

	// 2.6.2.2
	err := crdb.ExecTx(db, func(tx *sql.Tx) error {
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
