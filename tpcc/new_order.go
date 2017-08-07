// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Dona-Couch <andrew@cockroachlabs.com>

package main

import (
	"bytes"
	"runtime"

	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type ol struct {
	ol_i_id        int64
	ol_supply_w_id int64
	ol_quantity    int64
}

type it struct {
	i_id    int64
	i_price float64
	i_name  string
	i_data  string
}

type st struct {
	s_quantity   int64
	s_dist       string
	s_data       string
	s_ytd        int64
	s_order_cnt  int64
	s_remote_cnt int64
}

func debug() {
	buf := make([]byte, 0, 500)
	runtime.Stack(buf, true)
	fmt.Printf(bytes.NewBuffer(buf).String() + "\n")

	_, file, line, ok := runtime.Caller(0)
	if ok {
		fmt.Printf("crashing at %v:%v\n", file, line)
	}
	_, file, line, ok = runtime.Caller(1)
	if ok {
		fmt.Printf("crashing at %v:%v\n", file, line)
	}
}

// 2.4
func (t *Terminal) NewOrder(db *sql.DB) error {
	// 2.4.1.1
	w_id := t.w_id

	// 2.4.1.2
	d_id := t.rand.Rand(1, 10)
	c_id := t.rand.RandCId()

	// 2.4.1.3
	ol_cnt := t.rand.Rand(5, 15)

	// 2.4.1.4
	rbk := t.rand.Rand(1, 100)

	ols := make([]ol, ol_cnt)

	ol_all_local := 1

	// 2.4.1.5
	for i := int64(1); i <= ol_cnt; i++ {

		// 2.4.1.5.1
		var ol_i_id int64
		if i == ol_cnt && rbk == 1 {
			ol_i_id = INVALID_I_ID
		} else {
			ol_i_id = t.rand.RandOLIId()
		}

		// 2.4.1.5.2
		x := t.rand.Rand(1, 100)
		ol_supply_w_id := w_id
		if x == 1 && *W > 1 {
			ol_all_local = 0
			for ol_supply_w_id == w_id {
				ol_supply_w_id = t.rand.Rand(1, int64(*W))
			}
		}

		// 2.4.1.5.3
		ol_quantity := t.rand.Rand(1, 10)

		ols[i-1] = ol{
			ol_i_id:        ol_i_id,
			ol_supply_w_id: ol_supply_w_id,
			ol_quantity:    ol_quantity,
		}
	}

	err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		// 2.4.2.2
		var w_tax float64
		err := tx.QueryRow("SELECT w_tax FROM warehouse WHERE w_id = $1;", w_id).Scan(&w_tax)
		if err != nil {
			debug()
			return err
		}

		var d_tax float64
		var o_id int64
		err = tx.QueryRow("SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2;", w_id, d_id).Scan(&d_tax, &o_id)
		if err != nil {
			debug()
			return err
		}

		_, err = tx.Exec("UPDATE district SET d_next_o_id = $1 WHERE d_w_id = $2 AND d_id = $3;", o_id+1, w_id, d_id)
		if err != nil {
			debug()
			return err
		}

		var c_discount float64
		var c_last string
		var c_credit string
		err = tx.QueryRow("SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3;", w_id, d_id, c_id).Scan(&c_discount, &c_last, &c_credit)
		if err != nil {
			debug()
			return err
		}

		_, err = tx.Exec("INSERT INTO \"order\" VALUES ($1, $2, $3, $4, DEFAULT, NULL, $5, $6);", o_id, d_id, w_id, c_id, ol_cnt, ol_all_local)
		if err != nil {
			debug()
			return err
		}

		_, err = tx.Exec("INSERT INTO new_order VALUES ($1, $2, $3);", o_id, d_id, w_id)
		if err != nil {
			debug()
			return err
		}

		item_dollars := make([]string, ol_cnt)
		for i := int64(0); i < ol_cnt; i++ {
			item_dollars[i] = fmt.Sprintf("$%v", i+1)
		}
		item_places := strings.Join(item_dollars, ",")

		i_ids := make([]interface{}, ol_cnt)
		for i, line := range ols {
			i_ids[i] = line.ol_i_id // TODO: what if there are repeats?
		}

		item_query := fmt.Sprintf("SELECT i_id, i_price, i_name, i_data FROM item WHERE i_id IN (%s);", item_places)
		res, err := tx.Query(item_query, i_ids...)
		item_details := make(map[int64]it)
		for res.Next() {
			var i it
			err = res.Scan(&i.i_id, &i.i_price, &i.i_name, &i.i_data)
			if err != nil {
				debug()
				return err
			}
			item_details[i.i_id] = i
		}
		err = res.Err()
		if err != nil {
			debug()
			return err
		}

		stock_details := make([]st, ol_cnt)
		stock_query := fmt.Sprintf("SELECT s_quantity, s_dist_%02d, s_data, s_ytd, s_order_cnt, s_remote_cnt FROM stock WHERE s_i_id = $1 AND s_w_id = $2;", d_id)
		for i, line := range ols {
			var s st
			err = tx.QueryRow(stock_query, line.ol_i_id, line.ol_supply_w_id).Scan(&s.s_quantity, &s.s_dist, &s.s_data, &s.s_ytd, &s.s_order_cnt, &s.s_remote_cnt)
			if err != nil {
				fmt.Printf("tried to get stock for %v at %v\n", line.ol_i_id, line.ol_supply_w_id)
				fmt.Printf("using query: %s\n", stock_query)
				debug()
				return nil
				return err
			}
			stock_details[i] = s

			new_quantity := s.s_quantity - line.ol_quantity
			if s.s_quantity-line.ol_quantity < 10 {
				new_quantity = new_quantity + 91
			}
			new_ytd := s.s_ytd + line.ol_quantity
			new_order_cnt := s.s_order_cnt + 1
			new_remote_cnt := s.s_remote_cnt
			if line.ol_supply_w_id != w_id {
				new_remote_cnt++
			}

			_, err = tx.Exec("UPDATE stock SET s_quantity = $1, s_ytd = $2, s_order_cnt = $3, s_remote_cnt = $4 WHERE s_i_id = $5 AND s_w_id = $6;", new_quantity, new_ytd, new_order_cnt, new_remote_cnt, line.ol_i_id, line.ol_supply_w_id)
			if err != nil {
				debug()
				return err
			}
		}
		err = res.Err()
		if err != nil {
			debug()
			return err
		}

		total_amount := 0.0

		lines := make([]string, ol_cnt)
		for i, line := range ols {
			line_item := item_details[line.ol_i_id]
			line_stock := stock_details[i]

			ol_number := i + 1
			ol_amount := float64(line.ol_quantity) * line_item.i_price
			ol_dist_info := line_stock.s_dist

			brand_generic := "G"
			i_original := strings.Contains(line_item.i_data, "ORIGINAL")
			s_original := strings.Contains(line_stock.s_data, "ORIGINAL")
			if i_original && s_original {
				brand_generic = "B"
			}
			// TODO: something with brand_generic?
			_ = brand_generic

			total_amount = total_amount + ol_amount

			lines[i] = fmt.Sprintf("(%v,%v,%v,%v,%v,%v,NULL,%v,%0.2f,'%s')", o_id, d_id, w_id, ol_number, line.ol_i_id, line.ol_supply_w_id, line.ol_quantity, ol_amount, ol_dist_info)
		}
		_, err = tx.Exec("INSERT INTO order_line VALUES " + strings.Join(lines, ",") + ";")
		if err != nil {
			debug()
			return err
		}

		// TODO: something with total_amount?

		_ = w_tax
		_ = d_tax

		return nil
	})

	if err != nil {
		debug()
		return err
	}

	return nil
}
