// 4.3.3.1

package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/cockroach-go/crdb"
)

const BATCH_SIZE = 500

const ITEMS_COUNT = 100000
const ITEMS_INSERT = "INSERT INTO item VALUES "
const ITEMS_PLACES = "(%v,%v,'%s',%0.2f,'%s')"

func insertRows(db *sql.DB, prefix string, items []string) error {
	statement := prefix + strings.Join(items, ",")

	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		_, err := db.Exec(statement)
		return err
	})
}

func populateTable(
	db *sql.DB,
	rand Rand,
	table string,
	cardinality int,
	prefix string,
	maker func(rand Rand, count int) string,
) error {
	if *verbose {
		log.Printf("populating initial data for table %s\n", table)
	}

	rows := make([]string, 0, BATCH_SIZE)

	for count, i := 0, 0; count < cardinality; i++ {
		if i == BATCH_SIZE {
			err := insertRows(db, prefix, rows)
			if err != nil {
				return err
			}

			i = 0
			rows = rows[:0]
		}

		count++
		rows = append(
			rows,
			maker(rand, count),
		)
	}

	return insertRows(db, prefix, rows)
}

func makeItem(rand Rand, count int) string {
	i_id := count
	i_im_id := rand.Rand(1, 10000)
	i_name := rand.RandAString(14, 24)
	i_price := float64(rand.Rand(100, 10000)) / 100.0
	i_data := rand.RandAString(26, 50)

	has_original := rand.Rand(1, 10)
	if has_original == 1 {
		length := len(i_data)
		position := 50
		for position > 42 {
			position = int(rand.Rand(0, int64(length)))
		}
		rest := ""
		if position+8 < length {
			rest = i_data[position+8:]
		}
		i_data = i_data[:position] + "ORIGINAL" + rest
	}

	return fmt.Sprintf(ITEMS_PLACES, i_id, i_im_id, i_name, i_price, i_data)
}

const WAREHOUSES_INSERT string = "INSERT INTO warehouse VALUES "
const WAREHOUSES_PLACES string = "(%v,'%s','%s','%s','%s','%s','%s',%0.4f,%0.2f)"

func makeWarehouse(rand Rand, count int) string {
	w_id := count
	w_name := rand.RandAString(6, 10)
	w_street_1 := rand.RandAString(10, 20)
	w_street_2 := rand.RandAString(10, 20)
	w_city := rand.RandAString(10, 20)
	w_state := rand.RandAString(2, 2)
	w_zip := rand.RandZip()
	w_tax := float64(rand.Rand(0, 2000)) / 10000.0
	w_ytd := 300000.0

	return fmt.Sprintf(
		WAREHOUSES_PLACES, w_id, w_name, w_street_1, w_street_2,
		w_city, w_state, w_zip, w_tax, w_ytd,
	)
}

func Populate(db *sql.DB, rand Rand, W int) error {
	var table_data = [...]struct {
		name   string
		card   int
		prefix string
		maker  func(rand Rand, count int) string
	}{
		{"item", ITEMS_COUNT, ITEMS_INSERT, makeItem},
		{"warehouse", W, WAREHOUSES_INSERT, makeWarehouse},
	}

	var err error
	for _, t := range table_data {
		err = populateTable(db, rand, t.name, t.card, t.prefix, t.maker)
		if err != nil {
			return err
		}
	}
	return nil
}
