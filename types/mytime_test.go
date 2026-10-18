// Copyright 2016 PingCAP, Inc.
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

func TestWeekBehaviour(t *testing.T) {
	if weekBehaviourMondayFirst != weekBehaviour(1) {
		t.Fatalf("got %#v, want %#v", weekBehaviourMondayFirst, weekBehaviour(1))
	}
	if weekBehaviourYear != weekBehaviour(2) {
		t.Fatalf("got %#v, want %#v", weekBehaviourYear, weekBehaviour(2))
	}
	if weekBehaviourFirstWeekday != weekBehaviour(4) {
		t.Fatalf("got %#v, want %#v", weekBehaviourFirstWeekday, weekBehaviour(4))
	}

	if !weekBehaviour(1).test(weekBehaviourMondayFirst) {
		t.Errorf("expected true")
	}
	if !weekBehaviour(2).test(weekBehaviourYear) {
		t.Errorf("expected true")
	}
	if !weekBehaviour(4).test(weekBehaviourFirstWeekday) {
		t.Errorf("expected true")
	}
}

func TestWeek(t *testing.T) {
	tests := []struct {
		Input  MysqlTime
		Mode   int
		Expect int
	}{
		{MysqlTime{2008, 2, 20, 0, 0, 0, 0}, 0, 7},
		{MysqlTime{2008, 2, 20, 0, 0, 0, 0}, 1, 8},
		{MysqlTime{2008, 12, 31, 0, 0, 0, 0}, 1, 53},
	}

	for ith, tt := range tests {
		_, week := calcWeek(&tt.Input, weekMode(tt.Mode))
		if week != tt.Expect {
			t.Errorf("%d failed.", ith)
		}
	}
}

func TestCalcDaynr(t *testing.T) {
	if calcDaynr(0, 0, 0) != 0 {
		t.Fatalf("got %#v, want %#v", calcDaynr(0, 0, 0), 0)
	}
	if calcDaynr(9999, 12, 31) != 3652424 {
		t.Fatalf("got %#v, want %#v", calcDaynr(9999, 12, 31), 3652424)
	}
	if calcDaynr(1970, 1, 1) != 719528 {
		t.Fatalf("got %#v, want %#v", calcDaynr(1970, 1, 1), 719528)
	}
	if calcDaynr(2006, 12, 16) != 733026 {
		t.Fatalf("got %#v, want %#v", calcDaynr(2006, 12, 16), 733026)
	}
	if calcDaynr(10, 1, 2) != 3654 {
		t.Fatalf("got %#v, want %#v", calcDaynr(10, 1, 2), 3654)
	}
	if calcDaynr(2008, 2, 20) != 733457 {
		t.Fatalf("got %#v, want %#v", calcDaynr(2008, 2, 20), 733457)
	}
}

func TestCalcTimeDiff(t *testing.T) {
	tests := []struct {
		T1     MysqlTime
		T2     MysqlTime
		Sign   int
		Expect MysqlTime
	}{
		// calcTimeDiff can be used for month = 0.
		{
			MysqlTime{2006, 0, 1, 12, 23, 21, 0},
			MysqlTime{2006, 0, 3, 21, 23, 22, 0},
			1,
			MysqlTime{0, 0, 0, 57, 0, 1, 0},
		},
		{
			MysqlTime{0, 0, 0, 21, 23, 24, 0},
			MysqlTime{0, 0, 0, 11, 23, 22, 0},
			1,
			MysqlTime{0, 0, 0, 10, 0, 2, 0},
		},
		{
			MysqlTime{0, 0, 0, 1, 2, 3, 0},
			MysqlTime{0, 0, 0, 5, 2, 0, 0},
			-1,
			MysqlTime{0, 0, 0, 6, 4, 3, 0},
		},
	}

	for i, tt := range tests {
		seconds, microseconds, _ := calcTimeDiff(tt.T1, tt.T2, tt.Sign)
		var result MysqlTime
		calcTimeFromSec(&result, seconds, microseconds)
		if result != tt.Expect {
			t.Fatalf("%d failed.", i)
		}
	}
}

func TestCompareTime(t *testing.T) {
	tests := []struct {
		T1     MysqlTime
		T2     MysqlTime
		Expect int
	}{
		{MysqlTime{0, 0, 0, 0, 0, 0, 0}, MysqlTime{0, 0, 0, 0, 0, 0, 0}, 0},
		{MysqlTime{0, 0, 0, 0, 1, 0, 0}, MysqlTime{0, 0, 0, 0, 0, 0, 0}, 1},
		{MysqlTime{2006, 1, 2, 3, 4, 5, 6}, MysqlTime{2016, 1, 2, 3, 4, 5, 0}, -1},
		{MysqlTime{0, 0, 0, 11, 22, 33, 0}, MysqlTime{0, 0, 0, 12, 21, 33, 0}, -1},
		{MysqlTime{9999, 12, 30, 23, 59, 59, 999999}, MysqlTime{0, 1, 2, 3, 4, 5, 6}, 1},
	}

	for _, tt := range tests {
		if compareTime(tt.T1, tt.T2) != tt.Expect {
			t.Fatalf("got %#v, want %#v", compareTime(tt.T1, tt.T2), tt.Expect)
		}
		if compareTime(tt.T2, tt.T1) != -tt.Expect {
			t.Fatalf("got %#v, want %#v", compareTime(tt.T2, tt.T1), -tt.Expect)
		}
	}
}

func TestGetDateFromDaynr(t *testing.T) {
	tests := []struct {
		daynr uint
		year  uint
		month uint
		day   uint
	}{
		{730669, 2000, 7, 3},
		{720195, 1971, 10, 30},
		{719528, 1970, 01, 01},
		{719892, 1970, 12, 31},
		{730850, 2000, 12, 31},
		{730544, 2000, 2, 29},
		{204960, 561, 2, 28},
		{0, 0, 0, 0},
		{32, 0, 0, 0},
		{366, 1, 1, 1},
		{744729, 2038, 12, 31},
		{3652424, 9999, 12, 31},
	}

	for _, tt := range tests {
		yy, mm, dd := getDateFromDaynr(tt.daynr)
		if yy != tt.year {
			t.Fatalf("got %#v, want %#v", yy, tt.year)
		}
		if mm != tt.month {
			t.Fatalf("got %#v, want %#v", mm, tt.month)
		}
		if dd != tt.day {
			t.Fatalf("got %#v, want %#v", dd, tt.day)
		}
	}
}

func TestMixDateAndTime(t *testing.T) {
	tests := []struct {
		date   MysqlTime
		time   MysqlTime
		neg    bool
		expect MysqlTime
	}{
		{
			date:   MysqlTime{1896, 3, 4, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 12, 23, 24, 5},
			neg:    false,
			expect: MysqlTime{1896, 3, 4, 12, 23, 24, 5},
		},
		{
			date:   MysqlTime{1896, 3, 4, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 24, 23, 24, 5},
			neg:    false,
			expect: MysqlTime{1896, 3, 5, 0, 23, 24, 5},
		},
		{
			date:   MysqlTime{2016, 12, 31, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 24, 0, 0, 0},
			neg:    false,
			expect: MysqlTime{2017, 1, 1, 0, 0, 0, 0},
		},
		{
			date:   MysqlTime{2016, 12, 0, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 24, 0, 0, 0},
			neg:    false,
			expect: MysqlTime{2016, 12, 1, 0, 0, 0, 0},
		},
		{
			date:   MysqlTime{2017, 1, 12, 3, 23, 15, 0},
			time:   MysqlTime{0, 0, 0, 2, 21, 10, 0},
			neg:    true,
			expect: MysqlTime{2017, 1, 12, 1, 2, 5, 0},
		},
	}

	for ith, tt := range tests {
		mixDateAndTime(&tt.date, &tt.time, tt.neg)
		if compareTime(tt.date, tt.expect) != 0 {
			t.Fatalf("%d", ith)
		}
	}
}

func TestIsLeapYear(t *testing.T) {
	tests := []struct {
		T      MysqlTime
		Expect bool
	}{
		{MysqlTime{1960, 1, 1, 0, 0, 0, 0}, true},
		{MysqlTime{1963, 2, 21, 0, 0, 0, 0}, false},
		{MysqlTime{2008, 11, 25, 0, 0, 0, 0}, true},
		{MysqlTime{2017, 4, 24, 0, 0, 0, 0}, false},
		{MysqlTime{1988, 2, 29, 0, 0, 0, 0}, true},
		{MysqlTime{2000, 3, 15, 0, 0, 0, 0}, true},
		{MysqlTime{1992, 5, 3, 0, 0, 0, 0}, true},
		{MysqlTime{2024, 10, 1, 0, 0, 0, 0}, true},
		{MysqlTime{2016, 6, 29, 0, 0, 0, 0}, true},
		{MysqlTime{2015, 6, 29, 0, 0, 0, 0}, false},
		{MysqlTime{2014, 9, 31, 0, 0, 0, 0}, false},
		{MysqlTime{2001, 12, 7, 0, 0, 0, 0}, false},
		{MysqlTime{1989, 7, 6, 0, 0, 0, 0}, false},
	}

	for _, tt := range tests {
		if tt.T.IsLeapYear() != tt.Expect {
			t.Fatalf("got %#v, want %#v", tt.T.IsLeapYear(), tt.Expect)
		}
	}
}
func TestGetLastDay(t *testing.T) {
	tests := []struct {
		year        int
		month       int
		expectedDay int
	}{
		{2000, 1, 31},
		{2000, 2, 29},
		{2000, 4, 30},
		{1900, 2, 28},
		{1996, 2, 29},
	}

	for _, tt := range tests {
		day := GetLastDay(tt.year, tt.month)
		if day != tt.expectedDay {
			t.Fatalf("got %#v, want %#v", day, tt.expectedDay)
		}
	}
}
