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
	"fmt"
	"math/rand"
)

type Rand struct {
	generator *rand.Rand
}

// 2.1.5
func (r Rand) Rand(x, y int64) int64 {
	size := x - y + 1
	min := y
	if x < y {
		size = y - x + 1
		min = x
	}

	return min + r.generator.Int63n(size)
}

func makeRand(seed int64) Rand {
	return Rand{
		generator: rand.New(rand.NewSource(seed)),
	}
}

type nur struct {
	Rand
	c int64
}

// 2.1.6
func (r nur) nurand(a, x, y int64) int64 {
	return (((r.Rand.Rand(0, a) | r.Rand.Rand(x, y)) + r.c) % (y - x + 1)) + x
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

func makeFieldGenerator(seed, c, a, x, y int64) FieldGenerator {
 return FieldGenerator{
    nur: nur{
			Rand: makeRand(seed),
			c: c,
		},
		a: a,
		x: x,
		y: y,
	}
}

func C_ID(seed, c int64) FieldGenerator {
	return makeFieldGenerator(seed, c % 1024, 1023, 1, 3000)
}

func OL_I_ID(seed, c int64) FieldGenerator {
	return makeFieldGenerator(seed, c % 8192, 8191, 1, 100000)
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

type NameGenerator struct {
	Num FieldGenerator
}

func (g NameGenerator) Generate() string {
	i := g.Num.Generate()
	return syllables[i/100] + syllables[(i / 10) % 10] + syllables[i % 10]
}

func C_LAST(seed, c int64) NameGenerator {
	return NameGenerator{makeFieldGenerator(seed, c % 256, 255, 0, 999)}
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
			"The values for C used to generate C_LAST for loading and " +
			"running must differ by between 65 and 119.  Saw %v, values " +
			"were %v and %v.",
			cDelta,
			cLoadFinal,
			cRunFinal,
		)
	}
	return nil
}
