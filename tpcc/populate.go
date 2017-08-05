// 4.3.3.1

package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

const BATCH_SIZE = 500

var loadTime string = time.Now().Format("'2006-01-02T15:04:05'")

// Performs a one-indexed group by
func groupBy(group_size, count int) (item, group int) {
	index := count - 1
	i := index % group_size
	g := index / group_size
	return i + 1, g + 1
}

// Inserts the row values into the table
func insertRows(db *sql.DB, prefix string, items []string) error {
	statement := prefix + strings.Join(items, ",")

	return crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		_, err := db.Exec(statement)
		return err
	})
}

// Populate a table with a number of records
func populateTable(
	db *sql.DB,
	rand *Rand,
	table string,
	cardinality int,
	batch int,
	maker func(rand *Rand, count int) string,
) error {
	if *verbose {
		log.Printf("populating initial data for table %s\n", table)
	}

	prefix := fmt.Sprintf("INSERT INTO %s VALUES ", table)
	rows := make([]string, 0, batch)

	for count, i := 0, 0; count < cardinality; i++ {
		if i == batch {
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

const ITEMS_COUNT = 100000
const ITEMS_PLACES = "(%v,%v,'%s',%0.2f,'%s')"

func makeItem(rand *Rand, count int) string {
	i_id := count
	i_im_id := rand.Rand(1, 10000)
	i_name := rand.RandAString(14, 24)
	i_price := float64(rand.Rand(100, 10000)) / 100.0
	i_data := rand.RandData()

	return fmt.Sprintf(ITEMS_PLACES, i_id, i_im_id, i_name, i_price, i_data)
}

const WAREHOUSES_PLACES string = "(%v,'%s','%s','%s','%s','%s','%s',%0.4f,%0.2f)"

func makeWarehouse(rand *Rand, count int) string {
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

const STOCK_PER_WAREHOUSE int = ITEMS_COUNT
const STOCK_PLACES string = "(%v,%v,%v,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%v,%v,%v,'%s')"

func makeStock(rand *Rand, count int) string {
	s_i_id, s_w_id := groupBy(STOCK_PER_WAREHOUSE, count)
	s_quantity := rand.Rand(10, 100)
	s_dist_01 := rand.RandAString(24, 24)
	s_dist_02 := rand.RandAString(24, 24)
	s_dist_03 := rand.RandAString(24, 24)
	s_dist_04 := rand.RandAString(24, 24)
	s_dist_05 := rand.RandAString(24, 24)
	s_dist_06 := rand.RandAString(24, 24)
	s_dist_07 := rand.RandAString(24, 24)
	s_dist_08 := rand.RandAString(24, 24)
	s_dist_09 := rand.RandAString(24, 24)
	s_dist_10 := rand.RandAString(24, 24)
	s_ytd := 0
	s_order_cnt := 0
	s_remote_cnt := 0
	s_data := rand.RandData()

	return fmt.Sprintf(
		STOCK_PLACES, s_i_id, s_w_id, s_quantity,
		s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
		s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10,
		s_ytd, s_order_cnt, s_remote_cnt, s_data,
	)
}

const DISTRICTS_PER_WAREHOUSE int = 10
const DISTRICTS_PLACES string = "(%v,%v,'%s','%s','%s','%s','%s','%s',%0.4f,%0.2f,%v)"

func makeDistrict(rand *Rand, count int) string {
	d_id, d_w_id := groupBy(DISTRICTS_PER_WAREHOUSE, count)
	d_name := rand.RandAString(6, 10)
	d_street_1 := rand.RandAString(10, 20)
	d_street_2 := rand.RandAString(10, 20)
	d_city := rand.RandAString(10, 20)
	d_state := rand.RandAString(2, 2)
	d_zip := rand.RandZip()
	d_tax := float64(rand.Rand(0, 2000)) / 10000.0
	d_ytd := 30000.0
	d_next_o_id := ORDERS_PER_DISTRICT + 1

	return fmt.Sprintf(
		DISTRICTS_PLACES, d_id, d_w_id, d_name,
		d_street_1, d_street_2, d_city, d_state, d_zip,
		d_tax, d_ytd, d_next_o_id,
	)
}

const CUSTOMERS_PER_DISTRICT int = 3000
const CUSTOMERS_PER_WAREHOUSE int = CUSTOMERS_PER_DISTRICT * DISTRICTS_PER_WAREHOUSE
const CUSTOMERS_PLACES string = "(%v,%v,%v,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s',%0.2f,%0.4f,%0.2f,%0.2f,%v,%v,'%s')"

func makeCustomer(rand *Rand, count int) string {
	c_id, district := groupBy(CUSTOMERS_PER_DISTRICT, count)
	c_d_id, c_w_id := groupBy(DISTRICTS_PER_WAREHOUSE, district)

	c_last := ""
	if c_id <= 1000 {
		c_last = NumberToName(int64(c_id - 1))
	} else {
		c_last = rand.RandCLastLoad()
	}

	c_middle := "OE"
	c_first := rand.RandAString(8, 16)
	c_street_1 := rand.RandAString(10, 20)
	c_street_2 := rand.RandAString(10, 20)
	c_city := rand.RandAString(10, 20)
	c_state := rand.RandAString(2, 2)
	c_zip := rand.RandZip()
	c_phone := rand.RandNString(16, 16)
	c_since := loadTime

	c_credit := "GC"
	if rand.Rand(1, 10) == 1 {
		c_credit = "BC"
	}

	c_credit_lim := 50000.0
	c_discount := float64(rand.Rand(0, 5000)) / 10000.0
	c_balance := -10.0
	c_ytd_payment := 10.0
	c_payment_cnt := 1
	c_delivery_cnt := 0
	c_data := rand.RandAString(300, 500)

	return fmt.Sprintf(
		CUSTOMERS_PLACES, c_id, c_d_id, c_w_id, c_last, c_middle, c_first,
		c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
		c_since, c_credit, c_credit_lim, c_discount, c_balance,
		c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data,
	)
}

const HISTORY_PLACES string = "(%v,%v,%v,%v,%v,%s,%0.2f,'%s')"

func makeHistory(rand *Rand, count int) string {
	h_c_id, district := groupBy(CUSTOMERS_PER_DISTRICT, count)
	h_c_d_id, h_c_w_id := groupBy(DISTRICTS_PER_WAREHOUSE, district)
	h_d_id, h_w_id := h_c_d_id, h_c_w_id
	h_date := loadTime
	h_amount := 10.0
	h_data := rand.RandAString(12, 24)

	return fmt.Sprintf(
		HISTORY_PLACES, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data,
	)
}

var o_c_ids []int
var o_ol_cnts = make(map[int]map[int]map[int]int) // w_id - d_id - o_id - cnt

func putCount(w_id, d_id, o_id int, cnt int64) {
	w_cnts, ok := o_ol_cnts[w_id]
	if !ok {
		w_cnts = make(map[int]map[int]int)
		o_ol_cnts[w_id] = w_cnts
	}
	d_cnts, ok := w_cnts[d_id]
	if !ok {
		d_cnts = make(map[int]int)
		w_cnts[d_id] = d_cnts
	}
	d_cnts[o_id] = int(cnt)
}

const ORDERS_PER_DISTRICT int = CUSTOMERS_PER_DISTRICT
const ORDERS_PER_WAREHOUSE int = ORDERS_PER_DISTRICT * DISTRICTS_PER_WAREHOUSE
const ORDERS_PLACES string = "(%v,%v,%v,%v,%s,%v,%v,%v)"

func makeOrder(rand *Rand, count int) string {
	o_id, district := groupBy(ORDERS_PER_DISTRICT, count)
	o_d_id, o_w_id := groupBy(DISTRICTS_PER_WAREHOUSE, district)

	if o_id == 1 {
		o_c_ids = rand.Perm(1, CUSTOMERS_PER_DISTRICT)
	}
	o_c_id := o_c_ids[0]
	o_c_ids = o_c_ids[1:]

	o_entry_d := loadTime

	o_carrier_id := "NULL"
	if o_id <= NUM_ORDERS_BEFORE_NEWORDER {
		o_carrier_id = fmt.Sprintf("%v", rand.Rand(1, 10))
	}

	o_ol_cnt := rand.Rand(5, 15)
	putCount(o_w_id, o_d_id, o_id, o_ol_cnt)

	o_all_local := 1

	return fmt.Sprintf(
		ORDERS_PLACES, o_id, o_d_id, o_w_id, o_c_id,
		o_entry_d, o_carrier_id, o_ol_cnt, o_all_local,
	)
}

const NEWORDERS_PER_DISTRICT int = 900
const NEWORDERS_PER_WAREHOUSE int = NEWORDERS_PER_DISTRICT * DISTRICTS_PER_WAREHOUSE
const NEWORDERS_PLACES string = "(%v,%v,%v)"
const NUM_ORDERS_BEFORE_NEWORDER int = ORDERS_PER_DISTRICT - NEWORDERS_PER_DISTRICT

func makeNewOrder(rand *Rand, count int) string {
	order, district := groupBy(NEWORDERS_PER_DISTRICT, count)
	no_o_id := order + NUM_ORDERS_BEFORE_NEWORDER
	no_d_id, no_w_id := groupBy(DISTRICTS_PER_WAREHOUSE, district)

	return fmt.Sprintf(
		NEWORDERS_PLACES, no_o_id, no_d_id, no_w_id,
	)
}

func makeOrderLines(rand *Rand, count int) string {
	ol_o_id, district := groupBy(ORDERS_PER_DISTRICT, count)
	ol_d_id, ol_w_id := groupBy(DISTRICTS_PER_WAREHOUSE, district)

	cnt := o_ol_cnts[ol_w_id][ol_d_id][ol_o_id]
	values := make([]string, cnt)

	for i := 0; i < cnt; i++ {
		values[i] = makeOrderLine(rand, i+1, ol_o_id, ol_d_id, ol_w_id)
	}

	return strings.Join(values, ",")
}

const ORDER_LINE_PLACES string = "(%v,%v,%v,%v,%v,%v,%s,%v,%0.2f,'%s')"

func makeOrderLine(rand *Rand, ol_number, ol_o_id, ol_d_id, ol_w_id int) string {
	ol_i_id := rand.Rand(1, ITEMS_COUNT)
	ol_supply_w_id := ol_w_id

	ol_delivery_d := "NULL"
	if ol_o_id <= NUM_ORDERS_BEFORE_NEWORDER {
		ol_delivery_d = loadTime
	}

	ol_quantity := 5

	ol_amount := 0.0
	if ol_o_id > NUM_ORDERS_BEFORE_NEWORDER {
		ol_amount = float64(rand.Rand(1, 999999)) / 1000000.0
	}

	ol_dist_info := rand.RandAString(24, 24)

	return fmt.Sprintf(
		ORDER_LINE_PLACES,
		ol_o_id, ol_d_id, ol_w_id, ol_number,
		ol_i_id, ol_supply_w_id, ol_delivery_d,
		ol_quantity, ol_amount, ol_dist_info,
	)
}

func Populate(db *sql.DB, rand *Rand, W int) error {
	var table_data = [...]struct {
		name  string
		card  int
		maker func(rand *Rand, count int) string
		batch int
	}{
		{"item", ITEMS_COUNT, makeItem, BATCH_SIZE},
		{"warehouse", W, makeWarehouse, BATCH_SIZE},
		{"stock", W * STOCK_PER_WAREHOUSE, makeStock, BATCH_SIZE},
		{"district", W * DISTRICTS_PER_WAREHOUSE, makeDistrict, BATCH_SIZE},
		{"customer", W * CUSTOMERS_PER_WAREHOUSE, makeCustomer, BATCH_SIZE},
		{"history", W * CUSTOMERS_PER_WAREHOUSE, makeHistory, BATCH_SIZE},
		{"\"order\"", W * ORDERS_PER_WAREHOUSE, makeOrder, BATCH_SIZE},
		{"newOrder", W * NEWORDERS_PER_WAREHOUSE, makeNewOrder, BATCH_SIZE},
		{"orderLine", W * ORDERS_PER_WAREHOUSE, makeOrderLines, BATCH_SIZE / 10},
	}

	var err error
	for _, t := range table_data {
		err = populateTable(db, rand, t.name, t.card, t.batch, t.maker)
		if err != nil {
			return err
		}
	}
	return nil
}
