// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"testing"
)

func TestSet(t *testing.T) {
	elems := []string{"a", "b", "c", "d"}
	tbl := []struct {
		Name          string
		ExpectedValue uint64
		ExpectedName  string
	}{
		{"a", 1, "a"},
		{"a,b,a", 3, "a,b"},
		{"b,a", 3, "a,b"},
		{"a,b,c,d", 15, "a,b,c,d"},
		{"d", 8, "d"},
		{"", 0, ""},
		{"0", 0, ""},
	}

	for _, tt := range tbl {
		e, err := ParseSetName(elems, tt.Name)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if e.ToNumber() != float64(tt.ExpectedValue) {
			t.Fatalf("got %#v, want %#v", e.ToNumber(), float64(tt.ExpectedValue))
		}
		if e.String() != tt.ExpectedName {
			t.Fatalf("got %#v, want %#v", e.String(), tt.ExpectedName)
		}
	}

	tblNumber := []struct {
		Number       uint64
		ExpectedName string
	}{
		{0, ""},
		{1, "a"},
		{3, "a,b"},
		{9, "a,d"},
	}

	for _, tt := range tblNumber {
		e, err := ParseSetValue(elems, tt.Number)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if e.String() != tt.ExpectedName {
			t.Fatalf("got %#v, want %#v", e.String(), tt.ExpectedName)
		}
		if e.ToNumber() != float64(tt.Number) {
			t.Fatalf("got %#v, want %#v", e.ToNumber(), float64(tt.Number))
		}
	}

	tblErr := []string{
		"a.e",
		"e.f",
	}
	for _, tt := range tblErr {
		_, err := ParseSetName(elems, tt)
		if err == nil {
			t.Fatalf("expected error for %q", tt)
		}
	}

	tblNumberErr := []uint64{
		100, 16, 64,
	}
	for _, tt := range tblNumberErr {
		_, err := ParseSetValue(elems, tt)
		if err == nil {
			t.Fatalf("expected error for %#v", tt)
		}
	}
}
