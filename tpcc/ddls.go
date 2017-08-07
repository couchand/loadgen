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
	"database/sql"
	"log"
	"strings"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/pkg/errors"
)

type table int

const (
	warehouse table = iota
	district
	customer
	history
	item
	stock
	order
	new_order
	order_line
	numTables
)

var tableNames = [...]string{
	warehouse:  "warehouse",
	district:   "district",
	customer:   "customer",
	history:    "history",
	item:       "item",
	stock:      "stock",
	order:      "order",
	new_order:  "new_order",
	order_line: "order_line",
}

// 1.3.1
var createStmts = [...]string{
	warehouse: `
		CREATE TABLE warehouse (
			w_id              INT           NOT NULL,
			w_name            STRING(10)    NOT NULL,
			w_street_1        STRING(20)    NOT NULL,
			w_street_2        STRING(20)    NOT NULL,
			w_city            STRING(20)    NOT NULL,
			w_state           STRING(2)     NOT NULL,
			w_zip             STRING(9)     NOT NULL,
			w_tax             DECIMAL(4,4)  NOT NULL,
			w_ytd             DECIMAL(12,2) NOT NULL,

			PRIMARY KEY (w_id)
		);
	`,

	district: `
		CREATE TABLE district (
			d_id              INT           NOT NULL,
			d_w_id            INT           NOT NULL,
			d_name            STRING(10)    NOT NULL,
			d_street_1        STRING(20)    NOT NULL,
			d_street_2        STRING(20)    NOT NULL,
			d_city            STRING(20)    NOT NULL,
			d_state           STRING(2)     NOT NULL,
			d_zip             STRING(9)     NOT NULL,
			d_tax             DECIMAL(4,4)  NOT NULL,
			d_ytd             DECIMAL(12,2) NOT NULL,
			d_next_o_id       INT           NOT NULL,

			PRIMARY KEY (d_w_id, d_id),
			CONSTRAINT d_fk_warehouse FOREIGN KEY (d_w_id) REFERENCES warehouse
		);
	`,

	customer: `
		CREATE TABLE customer (
			c_id              INT           NOT NULL,
			c_d_id            INT           NOT NULL,
			c_w_id            INT           NOT NULL,
			c_first           STRING(16)    NOT NULL,
			c_middle          STRING(2)     NOT NULL,
			c_last            STRING(16)    NOT NULL,
			c_street_1        STRING(20)    NOT NULL,
			c_street_2        STRING(20)    NOT NULL,
			c_city            STRING(20)    NOT NULL,
			c_state           STRING(2)     NOT NULL,
			c_zip             STRING(9)     NOT NULL,
			c_phone           STRING(16)    NOT NULL,
			c_since           TIMESTAMPTZ   NOT NULL DEFAULT current_timestamp(),
			c_credit          STRING(2)     NOT NULL,
			c_credit_lim      DECIMAL(12,2) NOT NULL,
			c_discount        DECIMAL(4,4)  NOT NULL,
			c_balance         DECIMAL(12,2) NOT NULL,
			c_ytd_payment     DECIMAL(12,2) NOT NULL,
			c_payment_cnt     INT           NOT NULL,
			c_delivery_cnt    INT           NOT NULL,
			c_data            STRING(500)   NOT NULL,

			PRIMARY KEY (c_w_id, c_d_id, c_id),
			CONSTRAINT c_fk_district FOREIGN KEY
				(c_w_id, c_d_id) REFERENCES district
		);
	`,

	history: `
		CREATE TABLE history (
			h_c_id            INT           NOT NULL,
			h_c_d_id          INT           NOT NULL,
			h_c_w_id          INT           NOT NULL,
			h_d_id            INT           NOT NULL,
			h_w_id            INT           NOT NULL,
			h_date            TIMESTAMPTZ   NOT NULL DEFAULT current_timestamp(),
			h_amount          DECIMAL(6,2)  NOT NULL,
			h_data            STRING(24)    NOT NULL,

			PRIMARY KEY (h_c_w_id, h_c_d_id, h_c_id),
			CONSTRAINT h_fk_customer FOREIGN KEY
				(h_c_w_id, h_c_d_id, h_c_id) REFERENCES customer,
			CONSTRAINT h_fk_district FOREIGN KEY
				(h_w_id, h_d_id) REFERENCES district
		) INTERLEAVE IN PARENT customer (h_c_w_id, h_c_d_id, h_c_id);
	`,

	item: `
		CREATE TABLE item (
			i_id              INT           NOT NULL,
			i_im_id           INT           NOT NULL,
			i_name            STRING(24)    NOT NULL,
			i_price           DECIMAL(5,2)  NOT NULL,
			i_data            STRING(50)    NOT NULL,

			PRIMARY KEY (i_id)
		);
	`,

	stock: `
		CREATE TABLE stock (
			s_i_id            INT           NOT NULL,
			s_w_id            INT           NOT NULL,
			s_quantity        INT           NOT NULL,
			s_dist_01         STRING(24)    NOT NULL,
			s_dist_02         STRING(24)    NOT NULL,
			s_dist_03         STRING(24)    NOT NULL,
			s_dist_04         STRING(24)    NOT NULL,
			s_dist_05         STRING(24)    NOT NULL,
			s_dist_06         STRING(24)    NOT NULL,
			s_dist_07         STRING(24)    NOT NULL,
			s_dist_08         STRING(24)    NOT NULL,
			s_dist_09         STRING(24)    NOT NULL,
			s_dist_10         STRING(24)    NOT NULL,
			s_ytd             INT           NOT NULL,
			s_order_cnt       INT           NOT NULL,
			s_remote_cnt      INT           NOT NULL,
			s_data            STRING(50)    NOT NULL,

			PRIMARY KEY (s_w_id, s_i_id),
			CONSTRAINT s_fk_warehouse FOREIGN KEY
				(s_w_id) REFERENCES warehouse,
			CONSTRAINT s_fk_item FOREIGN KEY
				(s_i_id) REFERENCES item
		);
	`,

	// 2.4.1.6
	order: `
		CREATE TABLE "order" (
			o_id              INT           NOT NULL,
			o_d_id            INT           NOT NULL,
			o_w_id            INT           NOT NULL,
			o_c_id            INT           NOT NULL,
			o_entry_d         TIMESTAMPTZ   NOT NULL DEFAULT current_timestamp(),
			o_carrier_id      INT,
			o_ol_cnt          INT           NOT NULL,
			o_all_local       INT           NOT NULL,

			PRIMARY KEY (o_w_id, o_d_id, o_id),
			CONSTRAINT o_fk_customer FOREIGN KEY
				(o_w_id, o_d_id, o_c_id) REFERENCES customer
		);
	`,

	new_order: `
		CREATE TABLE new_order (
			no_o_id           INT           NOT NULL,
			no_d_id           INT           NOT NULL,
			no_w_id           INT           NOT NULL,

			PRIMARY KEY (no_w_id, no_d_id, no_o_id),
			CONSTRAINT no_fk_order FOREIGN KEY
				(no_w_id, no_d_id, no_o_id) REFERENCES "order"
		) INTERLEAVE IN PARENT "order" (no_w_id, no_d_id, no_o_id);
	`,

	order_line: `
		CREATE TABLE order_line (
			ol_o_id           INT           NOT NULL,
			ol_d_id           INT           NOT NULL,
			ol_w_id           INT           NOT NULL,
			ol_number         INT           NOT NULL,
			ol_i_id           INT           NOT NULL,
			ol_supply_w_id    INT           NOT NULL,
			ol_delivery_d     TIMESTAMPTZ,
			ol_quantity       INT           NOT NULL,
			ol_amount         DECIMAL(6,2)  NOT NULL,
			ol_dist_info      STRING(24)    NOT NULL,

			PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number),
			CONSTRAINT ol_fk_order FOREIGN KEY
				(ol_w_id, ol_d_id, ol_o_id) REFERENCES "order",
			CONSTRAINT ol_fk_stock FOREIGN KEY
				(ol_supply_w_id, ol_i_id) REFERENCES stock
		) INTERLEAVE IN PARENT "order" (ol_w_id, ol_d_id, ol_o_id);
	`,
}

var dropStmts = [...]string{
	"DROP TABLE IF EXISTS warehouse CASCADE",
	"DROP TABLE IF EXISTS district CASCADE",
	"DROP TABLE IF EXISTS customer CASCADE",
	"DROP TABLE IF EXISTS history CASCADE",
	"DROP TABLE IF EXISTS item CASCADE",
	"DROP TABLE IF EXISTS stock CASCADE",
	"DROP TABLE IF EXISTS \"order\" CASCADE",
	"DROP TABLE IF EXISTS new_order CASCADE",
	"DROP TABLE IF EXISTS order_line CASCADE",
}

func resolveTableTypeFromFileName(filename string) (table, error) {
	switch strings.Split(filename, ".")[0] {
	case "warehouse":
		return warehouse, nil
	case "district":
		return district, nil
	case "customer":
		return customer, nil
	case "history":
		return history, nil
	case "item":
		return item, nil
	case "stock":
		return stock, nil
	case "order":
		return order, nil
	case "new_order":
		return new_order, nil
	case "order_line":
		return order_line, nil
	default:
		return -1, errors.Errorf("filenames must be of the form tabletype.num.tbl, found: '%s'", filename)
	}
}

func createTables(db *sql.DB) error {
	if *drop {
		if *verbose {
			log.Println("dropping any existing tables")
		}

		for i, dropStmt := range dropStmts {
			if *verbose {
				log.Printf("dropping table %s\n", tableNames[i])
			}
			err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
				_, inErr := db.Exec(dropStmt)
				return inErr
			})
			if err != nil {
				if *verbose {
					log.Printf("Failed to drop table %s: %s\n", tableNames[i], err)
				}
				return err
			}
		}

		if *verbose {
			log.Println("finished dropping tables.")
		}
	}

	if *verbose {
		log.Println("creating tables")
	}

	for i, createStmt := range createStmts {
		if *verbose {
			log.Printf("creating table %s\n", tableNames[i])
		}
		err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			_, inErr := db.Exec(createStmt)
			return inErr
		})
		if err != nil {
			if *verbose {
				log.Printf("Failed to create table %s: %s\n", tableNames[i], err)
			}
			return err
		}
	}
	if *verbose {
		log.Println("finished creating tables.")
	}

	return nil
}
