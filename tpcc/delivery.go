package main

import (
	"database/sql"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// 2.7
func (t *Terminal) Delivery(db *sql.DB) error {
	// TODO: implement

	// 2.7.2.2
	err := crdb.ExecTx(db, func(tx *sql.Tx) error {
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
