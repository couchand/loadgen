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

type ol struct {
	ol_i_id        int64
	ol_supply_w_id int64
	ol_quantity    int64
}

// 2.4
func (t *Terminal) NewOrder() {
	// 2.4.1.1
	w_id := t.w_id

	// 2.4.1.2
	d_id := t.rand.Rand(1, 10)
  c_id := t.c_id.Generate()

	// 2.4.1.3
	ol_cnt := t.rand.Rand(5, 15)

	// 2.4.1.4
	rbk := t.rand.Rand(1, 100)

	ols := make([]ol, ol_cnt)

	// 2.4.1.5
	for i := int64(1); i <= ol_cnt; i++ {

		// 2.4.1.5.1
		var ol_i_id int64
		if i == ol_cnt && rbk == 1 {
			ol_i_id = INVALID_I_ID
		} else {
			ol_i_id = t.ol_i_id.Generate()
		}

		// 2.4.1.5.2
		x := t.rand.Rand(1, 100)
		ol_supply_w_id := w_id
		if x == 1 && *W > 1 {
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

	_ = w_id
	_ = d_id
	_ = c_id
	_ = ols
}
