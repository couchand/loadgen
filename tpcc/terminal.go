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

type Terminal struct {
	rand Rand

	w_id    int64
	c_last  NameGenerator
	c_id    FieldGenerator
	ol_i_id FieldGenerator
}

func makeTerminal(seed, c_last, c_id, ol_i_id, w_id int64) *Terminal {
	rand := makeRand(seed)
	return &Terminal{
		rand:    rand,
		w_id:    w_id,
		c_last:  C_LAST(rand, c_last),
		c_id:    C_ID(rand, c_id),
		ol_i_id: OL_I_ID(rand, ol_i_id),
	}
}
