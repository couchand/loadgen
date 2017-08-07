package main

import (
	"database/sql"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// 2.8
func (t *Terminal) StockLevel(db *sql.DB) error {
	// 2.8.1.1
	w_id := t.w_id
	d_id := t.d_id

	// 2.8.12
	threshold := t.rand.Rand(10, 20)

	err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		// 2.8.2.2
		var d_next_o_id int64
		err := tx.QueryRow("SELECT d_next_o_id FROM district where d_id = $1 AND d_w_id = $2", d_id, w_id).Scan(&d_next_o_id)
		if err != nil {
			return err
		}

		min_o_id := d_next_o_id - 20
		max_o_id := d_next_o_id

		stock_level_query := `
			SELECT count(*) FROM stock
			WHERE s_w_id = $1 AND s_quantity < $5 AND s_i_id IN (
				SELECT DISTINCT ol_i_id FROM order_line
				WHERE ol_w_id = $1 AND ol_d_id = $2
				AND ol_o_id >= $3 AND ol_o_id < $4
			)
    `
		var low_stock int64
		err = tx.QueryRow(stock_level_query, w_id, d_id, min_o_id, max_o_id, threshold).Scan(&low_stock)

		// TODO: something with low_stock
		_ = low_stock

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
