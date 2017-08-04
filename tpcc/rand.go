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
	"fmt"
	"math/rand"
)

// Uniform random
type RandSource struct {
	generator *rand.Rand
}

func MakeRandSource(seed int64) RandSource {
	return RandSource{
		generator: rand.New(rand.NewSource(seed)),
	}
}

// 2.1.5
func (r RandSource) Rand(x, y int64) int64 {
	size := x - y + 1
	min := y
	if x < y {
		size = y - x + 1
		min = x
	}

	return min + r.generator.Int63n(size)
}

// 4.3.2.2
func (r RandSource) randString(x, y int64, a, z rune) string {
	length := r.Rand(x, y)
	buf := bytes.NewBuffer(make([]byte, 0, length))

	for i := int64(0); i < length; i++ {
		buf.WriteRune(rune(r.Rand(int64(a), int64(z))))
	}

	return buf.String()
}

func (r RandSource) RandAString(x, y int64) string {
	return r.randString(x, y, 'A', 'Z')
}

func (r RandSource) RandNString(x, y int64) string {
	return r.randString(x, y, '0', '9')
}

// 4.3.2.6
func (r RandSource) Perm(x, y int) []int {
	size := x - y + 1
	min := y
	if x < y {
		size = y - x + 1
		min = x
	}

	nums := r.generator.Perm(size)

	for i, v := range nums {
		nums[i] = min + v
	}

	return nums
}

// 4.3.2.7
func (r RandSource) RandZip() string {
	return r.RandNString(4, 4) + "11111"
}

// 4.3.3.1
func (r RandSource) RandData() string {
	data := r.RandAString(26, 50)

	has_original := r.Rand(1, 10)
	if has_original == 1 {
		length := len(data)
		position := 50
		for position > 42 {
			position = int(r.Rand(0, int64(length)))
		}
		rest := ""
		if position+8 < length {
			rest = data[position+8:]
		}
		data = data[:position] + "ORIGINAL" + rest
	}

	return data
}

// Non-uniform random
type nur struct {
	r RandSource
	c int64
}

// 2.1.6
func (r nur) nurand(a, x, y int64) int64 {
	return (((r.r.Rand(0, a) | r.r.Rand(x, y)) + r.c) % (y - x + 1)) + x
}

type FieldGenerator struct {
	nur
	a int64
	x int64
	y int64
}

func (f FieldGenerator) Generate() int64 {
	return f.nurand(f.a, f.x, f.y)
}

func makeFieldGenerator(r RandSource, c, a, x, y int64) FieldGenerator {
	return FieldGenerator{
		nur: nur{
			r: r,
			c: c,
		},
		a: a,
		x: x,
		y: y,
	}
}

func C_ID(r RandSource, c int64) FieldGenerator {
	return makeFieldGenerator(r, c%1024, 1023, 1, 3000)
}

func OL_I_ID(r RandSource, c int64) FieldGenerator {
	return makeFieldGenerator(r, c%8192, 8191, 1, 100000)
}

// 2.4.1.5.1
const INVALID_I_ID = 100001

// 4.3.2.3
var syllables = [...]string{
	"BAR",
	"OUGHT",
	"ABLE",
	"PRI",
	"PRES",
	"ESE",
	"ANTI",
	"CALLY",
	"ATION",
	"EING",
}

func NumberToName(i int64) string {
	return syllables[(i/100)%10] + syllables[(i/10)%10] + syllables[i%10]
}

type NameGenerator struct {
	Num FieldGenerator
}

func (g NameGenerator) Generate() string {
	i := g.Num.Generate()
	return NumberToName(i)
}

func C_LAST(r RandSource, c int64) NameGenerator {
	return NameGenerator{makeFieldGenerator(r, c%256, 255, 0, 999)}
}

// 2.1.6.1
func ValidateC_LAST(cLoad, cRun int64) error {
	cLoadFinal := cLoad % 256
	cRunFinal := cRun % 256

	cDelta := cLoadFinal - cRunFinal
	if cLoadFinal < cRunFinal {
		cDelta = cRunFinal - cLoadFinal
	}

	if cDelta < 65 || cDelta > 119 {
		return fmt.Errorf(
			"The values for C used to generate C_LAST for loading and "+
				"running must differ by between 65 and 119.  Saw %v, values "+
				"were %v and %v.",
			cDelta,
			cLoadFinal,
			cRunFinal,
		)
	}
	return nil
}

type Rand struct {
	RandSource
	c_last_load NameGenerator
	c_last_run  NameGenerator
	c_id        FieldGenerator
	ol_i_id     FieldGenerator
}

func (r *Rand) RandCLastLoad() string {
	return r.c_last_load.Generate()
}

func (r *Rand) RandCLastRun() string {
	return r.c_last_run.Generate()
}

func (r *Rand) RandCId() int64 {
	return r.c_id.Generate()
}

func (r *Rand) RandOLIId() int64 {
	return r.ol_i_id.Generate()
}

func MakeRand(ur RandSource, c_last_load_c, c_last_run_c, c_id_c, ol_i_id_c int64) (*Rand, error) {
	err := ValidateC_LAST(c_last_load_c, c_last_run_c)
	if err != nil {
		return nil, err
	}

	return &Rand{
		RandSource:  ur,
		c_last_load: C_LAST(ur, c_last_load_c),
		c_last_run:  C_LAST(ur, c_last_run_c),
		c_id:        C_ID(ur, c_id_c),
		ol_i_id:     OL_I_ID(ur, ol_i_id_c),
	}, nil
}
