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
	"time"

	"github.com/mia0x75/parser/mysql"
	"github.com/mia0x75/parser/types"
	. "github.com/pingcap/check"
)

var _ = Suite(&testTimeSuite{})

type testTimeSuite struct {
}

func (s *testTimeSuite) TestYear(c *C) {
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
		t, err := types.ParseYear(test.Input)
		c.Assert(err, IsNil)
		c.Assert(t, Equals, test.Expect)
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
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
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
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.Expect)
	}

	numYears := []struct {
		Year   int64
		Expect int64
	}{
		{0, 0},
	}
	for _, test := range numYears {
		res, err := types.AdjustYear(test.Year, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.Expect)
	}
}

func (s *testTimeSuite) getLocation(c *C) *time.Location {
	locations := []string{"Asia/Shanghai", "Europe/Berlin"}
	timeFormat := "Jan 2, 2006 at 3:04pm (MST)"

	z, err := time.LoadLocation(locations[0])
	c.Assert(err, IsNil)

	t1, err := time.ParseInLocation(timeFormat, "Jul 9, 2012 at 5:02am (CEST)", z)
	c.Assert(err, IsNil)
	t2, err := time.Parse(timeFormat, "Jul 9, 2012 at 5:02am (CEST)")
	c.Assert(err, IsNil)

	if t1.Equal(t2) {
		z, err = time.LoadLocation(locations[1])
		c.Assert(err, IsNil)
	}

	return z
}

func (s *testTimeSuite) TestParseFrac(c *C) {
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

	for _, t := range tbl {
		v, overflow, err := types.ParseFrac(t.S, t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Ret)
		c.Assert(overflow, Equals, t.Overflow)
	}
}

func (s *testTimeSuite) TestParseDateFormat(c *C) {
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

	for _, t := range tbl {
		r := types.ParseDateFormat(t.Input)
		c.Assert(r, DeepEquals, t.Result)
	}
}

func (s *testTimeSuite) TestTamestampDiff(c *C) {
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
		c.Assert(types.TimestampDiff(test.unit, t1, t2), Equals, test.expect)
	}
}

func (s *testTimeSuite) TestDateFSP(c *C) {
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
		c.Assert(types.DateFSP(test.date), Equals, test.expect)
	}
}

func (s *testTimeSuite) TestConvertTimeZone(c *C) {
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
		var t types.Time
		t.Time = test.input
		t.ConvertTimeZone(test.from, test.to)
		c.Assert(t.Compare(types.Time{Time: test.expect}), Equals, 0)
	}
}

func (s *testTimeSuite) TestTruncateOverflowMySQLTime(c *C) {
	t := types.MaxTime + 1
	res, err := types.TruncateOverflowMySQLTime(t)
	c.Assert(types.ErrTruncatedWrongVal.Equal(err), IsTrue)
	c.Assert(res, Equals, types.MaxTime)

	t = types.MinTime - 1
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(types.ErrTruncatedWrongVal.Equal(err), IsTrue)
	c.Assert(res, Equals, types.MinTime)

	t = types.MaxTime
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MaxTime)

	t = types.MinTime
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MinTime)

	t = types.MaxTime - 1
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MaxTime-1)

	t = types.MinTime + 1
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MinTime+1)
}
