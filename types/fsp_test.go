// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"regexp"
	"strconv"
	"testing"
)

func TestCheckFsp(t *testing.T) {
	t.Parallel()
	obtained, err := CheckFsp(UnspecifiedFsp)
	if obtained != DefaultFsp {
		t.Fatalf("got %#v, want %#v", obtained, DefaultFsp)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	obtained, err = CheckFsp(-2019)
	if obtained != DefaultFsp {
		t.Fatalf("got %#v, want %#v", obtained, DefaultFsp)
	}
	if err == nil || !regexp.MustCompile("Invalid fsp -2019").MatchString(err.Error()) {
		t.Fatalf("got %#v, want match %#v", err, "Invalid fsp -2019")
	}

	obtained, err = CheckFsp(MinFsp - 4294967296)
	if obtained != DefaultFsp {
		t.Fatalf("got %#v, want %#v", obtained, DefaultFsp)
	}
	if err == nil || !regexp.MustCompile("Invalid fsp "+strconv.Itoa(MinFsp-4294967296)).MatchString(err.Error()) {
		t.Fatalf("got %#v, want match %#v", err, "Invalid fsp "+strconv.Itoa(MinFsp-4294967296))
	}

	// UnspecifiedFsp
	obtained, err = CheckFsp(-1)
	if obtained != DefaultFsp {
		t.Fatalf("got %#v, want %#v", obtained, DefaultFsp)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	obtained, err = CheckFsp(MaxFsp + 1)
	if obtained != DefaultFsp {
		t.Fatalf("got %#v, want %#v", obtained, DefaultFsp)
	}
	if err == nil || !regexp.MustCompile("Invalid fsp "+strconv.Itoa(MaxFsp+1)).MatchString(err.Error()) {
		t.Fatalf("got %#v, want match %#v", err, "Invalid fsp "+strconv.Itoa(MaxFsp+1))
	}

	obtained, err = CheckFsp(MaxFsp + 2019)
	if obtained != DefaultFsp {
		t.Fatalf("got %#v, want %#v", obtained, DefaultFsp)
	}
	if err == nil || !regexp.MustCompile("Invalid fsp "+strconv.Itoa(MaxFsp+2019)).MatchString(err.Error()) {
		t.Fatalf("got %#v, want match %#v", err, "Invalid fsp "+strconv.Itoa(MaxFsp+2019))
	}

	obtained, err = CheckFsp(MaxFsp + 4294967296)
	if obtained != DefaultFsp {
		t.Fatalf("got %#v, want %#v", obtained, DefaultFsp)
	}
	if err == nil || !regexp.MustCompile("Invalid fsp "+strconv.Itoa(MaxFsp+4294967296)).MatchString(err.Error()) {
		t.Fatalf("got %#v, want match %#v", err, "Invalid fsp "+strconv.Itoa(MaxFsp+4294967296))
	}

	obtained, err = CheckFsp((MaxFsp + MinFsp) / 2)
	if obtained != (MaxFsp+MinFsp)/2 {
		t.Fatalf("got %#v, want %#v", obtained, (MaxFsp+MinFsp)/2)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	obtained, err = CheckFsp(5)
	if obtained != 5 {
		t.Fatalf("got %#v, want %#v", obtained, 5)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}
}

func TestFsp_ParseFrac(t *testing.T) {
	t.Parallel()
	obtained, overflow, err := ParseFrac("", 5)
	if obtained != 0 {
		t.Fatalf("got %#v, want %#v", obtained, 0)
	}
	if overflow != false {
		t.Fatalf("got %#v, want %#v", overflow, false)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	obtained, overflow, err = ParseFrac("999", 200)
	if obtained != 0 {
		t.Fatalf("got %#v, want %#v", obtained, 0)
	}
	if overflow != false {
		t.Fatalf("got %#v, want %#v", overflow, false)
	}
	if err == nil || !regexp.MustCompile("Invalid fsp .*").MatchString(err.Error()) {
		t.Fatalf("got %#v, want match %#v", err, "Invalid fsp .*")
	}

	obtained, overflow, err = ParseFrac("NotNum", MaxFsp)
	if obtained != 0 {
		t.Fatalf("got %#v, want %#v", obtained, 0)
	}
	if overflow != false {
		t.Fatalf("got %#v, want %#v", overflow, false)
	}
	if err == nil || !regexp.MustCompile("strconv.ParseInt:.*").MatchString(err.Error()) {
		t.Fatalf("got %#v, want match %#v", err, "strconv.ParseInt:.*")
	}

	obtained, overflow, err = ParseFrac("1235", 6)
	if obtained != 123500 {
		t.Fatalf("got %#v, want %#v", obtained, 123500)
	}
	if overflow != false {
		t.Fatalf("got %#v, want %#v", overflow, false)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	obtained, overflow, err = ParseFrac("123456", 4)
	if obtained != 123500 {
		t.Fatalf("got %#v, want %#v", obtained, 123500)
	}
	if overflow != false {
		t.Fatalf("got %#v, want %#v", overflow, false)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	// 1236 round 3 -> 124 -> 124000
	obtained, overflow, err = ParseFrac("1236", 3)
	if obtained != 124000 {
		t.Fatalf("got %#v, want %#v", obtained, 124000)
	}
	if overflow != false {
		t.Fatalf("got %#v, want %#v", overflow, false)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	// 03123 round 2 -> 3 -> 30000
	obtained, overflow, err = ParseFrac("0312", 2)
	if obtained != 30000 {
		t.Fatalf("got %#v, want %#v", obtained, 30000)
	}
	if overflow != false {
		t.Fatalf("got %#v, want %#v", overflow, false)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}

	// 999 round 2 -> 100 -> overflow
	obtained, overflow, err = ParseFrac("999", 2)
	if obtained != 0 {
		t.Fatalf("got %#v, want %#v", obtained, 0)
	}
	if overflow != true {
		t.Fatalf("got %#v, want %#v", overflow, true)
	}
	if err != nil {
		t.Fatalf("got %#v, want nil", err)
	}
}

func TestAlignFrac(t *testing.T) {
	t.Parallel()
	obtained := alignFrac("100", 6)
	if obtained != "100000" {
		t.Fatalf("got %#v, want %#v", obtained, "100000")
	}
	obtained = alignFrac("10000000000", 6)
	if obtained != "10000000000" {
		t.Fatalf("got %#v, want %#v", obtained, "10000000000")
	}
}
