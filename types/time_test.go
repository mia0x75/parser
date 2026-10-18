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

package types_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/mia0x75/parser/mysql"
	"github.com/mia0x75/parser/types"
)

func TestYear(t *testing.T) {
	table := []struct {
		Input  string
		Expect int16
	}{
		{"1990", 1990},
		{"10", 2010},
		{"0", 2000},
		{"99", 1999},
	}

	for _, test := range table {
		year, err := types.ParseYear(test.Input)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if year != test.Expect {
			t.Fatalf("got %#v, want %#v", year, test.Expect)
		}
	}

	valids := []struct {
		Year   int64
		Expect bool
	}{
		{2000, true},
		{20000, false},
		{0, true},
		{-1, false},
	}

	for _, test := range valids {
		_, err := types.AdjustYear(test.Year, false)
		if test.Expect {
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
		} else {
			if err == nil {
				t.Fatalf("expected error for %#v", test.Year)
			}
		}
	}

	strYears := []struct {
		Year   int64
		Expect int64
	}{
		{0, 2000},
	}
	for _, test := range strYears {
		res, err := types.AdjustYear(test.Year, true)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if res != test.Expect {
			t.Fatalf("got %#v, want %#v", res, test.Expect)
		}
	}

	numYears := []struct {
		Year   int64
		Expect int64
	}{
		{0, 0},
	}
	for _, test := range numYears {
		res, err := types.AdjustYear(test.Year, false)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if res != test.Expect {
			t.Fatalf("got %#v, want %#v", res, test.Expect)
		}
	}
}

func getLocation(t *testing.T) *time.Location {
	locations := []string{"Asia/Shanghai", "Europe/Berlin"}
	timeFormat := "Jan 2, 2006 at 3:04pm (MST)"

	z, err := time.LoadLocation(locations[0])
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	t1, err := time.ParseInLocation(timeFormat, "Jul 9, 2012 at 5:02am (CEST)", z)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	t2, err := time.Parse(timeFormat, "Jul 9, 2012 at 5:02am (CEST)")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if t1.Equal(t2) {
		z, err = time.LoadLocation(locations[1])
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
	}

	return z
}

func TestTimeSuite_ParseFrac(t *testing.T) {
	tbl := []struct {
		S        string
		Fsp      int
		Ret      int
		Overflow bool
	}{
		// Round when fsp < string length.
		{"1234567", 0, 0, false},
		{"1234567", 1, 100000, false},
		{"0000567", 5, 60, false},
		{"1234567", 5, 123460, false},
		{"1234567", 6, 123457, false},
		// Fill 0 when fsp > string length.
		{"123", 4, 123000, false},
		{"123", 5, 123000, false},
		{"123", 6, 123000, false},
		{"11", 6, 110000, false},
		{"01", 3, 10000, false},
		{"012", 4, 12000, false},
		{"0123", 5, 12300, false},
		// Overflow
		{"9999999", 6, 0, true},
		{"999999", 5, 0, true},
		{"999", 2, 0, true},
		{"999", 3, 999000, false},
	}

	for _, tt := range tbl {
		v, overflow, err := types.ParseFrac(tt.S, tt.Fsp)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if v != tt.Ret {
			t.Fatalf("got %#v, want %#v", v, tt.Ret)
		}
		if overflow != tt.Overflow {
			t.Fatalf("got %#v, want %#v", overflow, tt.Overflow)
		}
	}
}

func TestParseDateFormat(t *testing.T) {
	tbl := []struct {
		Input  string
		Result []string
	}{
		{"2011-11-11 10:10:10.123456", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"  2011-11-11 10:10:10.123456  ", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"2011-11-11 10", []string{"2011", "11", "11", "10"}},
		{"2011-11-11T10:10:10.123456", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"2011:11:11T10:10:10.123456", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"xx2011-11-11 10:10:10", nil},
		{"T10:10:10", nil},
		{"2011-11-11x", nil},
		{"2011-11-11  10:10:10", nil},
		{"xxx 10:10:10", nil},
	}

	for _, tt := range tbl {
		r := types.ParseDateFormat(tt.Input)
		if !reflect.DeepEqual(r, tt.Result) {
			t.Fatalf("got %#v, want %#v", r, tt.Result)
		}
	}
}

func TestTamestampDiff(t *testing.T) {
	tests := []struct {
		unit   string
		t1     types.MysqlTime
		t2     types.MysqlTime
		expect int64
	}{
		{"MONTH", types.FromDate(2002, 5, 30, 0, 0, 0, 0), types.FromDate(2001, 1, 1, 0, 0, 0, 0), -16},
		{"YEAR", types.FromDate(2002, 5, 1, 0, 0, 0, 0), types.FromDate(2001, 1, 1, 0, 0, 0, 0), -1},
		{"MINUTE", types.FromDate(2003, 2, 1, 0, 0, 0, 0), types.FromDate(2003, 5, 1, 12, 5, 55, 0), 128885},
		{"MICROSECOND", types.FromDate(2002, 5, 30, 0, 0, 0, 0), types.FromDate(2002, 5, 30, 0, 13, 25, 0), 805000000},
		{"MICROSECOND", types.FromDate(2000, 1, 1, 0, 0, 0, 12345), types.FromDate(2000, 1, 1, 0, 0, 45, 32), 44987687},
		{"QUARTER", types.FromDate(2000, 1, 12, 0, 0, 0, 0), types.FromDate(2016, 1, 1, 0, 0, 0, 0), 63},
		{"QUARTER", types.FromDate(2016, 1, 1, 0, 0, 0, 0), types.FromDate(2000, 1, 12, 0, 0, 0, 0), -63},
	}

	for _, test := range tests {
		t1 := types.Time{
			Time: test.t1,
			Type: mysql.TypeDatetime,
			Fsp:  6,
		}
		t2 := types.Time{
			Time: test.t2,
			Type: mysql.TypeDatetime,
			Fsp:  6,
		}
		if got := types.TimestampDiff(test.unit, t1, t2); got != test.expect {
			t.Fatalf("got %#v, want %#v", got, test.expect)
		}
	}
}

func TestDateFSP(t *testing.T) {
	tests := []struct {
		date   string
		expect int
	}{
		{"2004-01-01 12:00:00.111", 3},
		{"2004-01-01 12:00:00.11", 2},
		{"2004-01-01 12:00:00.111111", 6},
		{"2004-01-01 12:00:00", 0},
	}

	for _, test := range tests {
		if got := types.DateFSP(test.date); got != test.expect {
			t.Fatalf("got %#v, want %#v", got, test.expect)
		}
	}
}

func TestConvertTimeZone(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	tests := []struct {
		input  types.MysqlTime
		from   *time.Location
		to     *time.Location
		expect types.MysqlTime
	}{
		{types.FromDate(2017, 1, 1, 0, 0, 0, 0), time.UTC, loc, types.FromDate(2017, 1, 1, 8, 0, 0, 0)},
		{types.FromDate(2017, 1, 1, 8, 0, 0, 0), loc, time.UTC, types.FromDate(2017, 1, 1, 0, 0, 0, 0)},
		{types.FromDate(0, 0, 0, 0, 0, 0, 0), loc, time.UTC, types.FromDate(0, 0, 0, 0, 0, 0, 0)},
	}

	for _, test := range tests {
		var tm types.Time
		tm.Time = test.input
		tm.ConvertTimeZone(test.from, test.to)
		if tm.Compare(types.Time{Time: test.expect}) != 0 {
			t.Fatalf("got %#v, want %#v", tm.Compare(types.Time{Time: test.expect}), 0)
		}
	}
}

func TestTruncateOverflowMySQLTime(t *testing.T) {
	v := types.MaxTime + 1
	res, err := types.TruncateOverflowMySQLTime(v)
	if !types.ErrTruncatedWrongVal.Equal(err) {
		t.Fatalf("got %#v, want %#v", err, types.ErrTruncatedWrongVal)
	}
	if res != types.MaxTime {
		t.Fatalf("got %#v, want %#v", res, types.MaxTime)
	}

	v = types.MinTime - 1
	res, err = types.TruncateOverflowMySQLTime(v)
	if !types.ErrTruncatedWrongVal.Equal(err) {
		t.Fatalf("got %#v, want %#v", err, types.ErrTruncatedWrongVal)
	}
	if res != types.MinTime {
		t.Fatalf("got %#v, want %#v", res, types.MinTime)
	}

	v = types.MaxTime
	res, err = types.TruncateOverflowMySQLTime(v)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if res != types.MaxTime {
		t.Fatalf("got %#v, want %#v", res, types.MaxTime)
	}

	v = types.MinTime
	res, err = types.TruncateOverflowMySQLTime(v)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if res != types.MinTime {
		t.Fatalf("got %#v, want %#v", res, types.MinTime)
	}

	v = types.MaxTime - 1
	res, err = types.TruncateOverflowMySQLTime(v)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if res != types.MaxTime-1 {
		t.Fatalf("got %#v, want %#v", res, types.MaxTime-1)
	}

	v = types.MinTime + 1
	res, err = types.TruncateOverflowMySQLTime(v)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if res != types.MinTime+1 {
		t.Fatalf("got %#v, want %#v", res, types.MinTime+1)
	}
}
