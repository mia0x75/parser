// Copyright 2017 PingCAP, Inc.
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

package types_test

import (
	"testing"

	"github.com/mia0x75/parser/mysql"
)

func TestGetSQLMode(t *testing.T) {
	positiveCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE"},
		{",,NO_ZERO_DATE"},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE"},
		{""},
		{", "},
		{","},
	}

	for _, tt := range positiveCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(tt.arg))
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
	}

	negativeCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE, NO_ZERO_IN_DATE"},
		{"NO_ZERO_DATE,adfadsdfasdfads"},
		{", ,NO_ZERO_DATE"},
		{" ,"},
	}

	for _, tt := range negativeCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(tt.arg))
		if err == nil {
			t.Fatalf("expected error for %q", tt.arg)
		}
	}
}

func TestSQLMode(t *testing.T) {
	tests := []struct {
		arg                           string
		hasNoZeroDateMode             bool
		hasNoZeroInDateMode           bool
		hasErrorForDivisionByZeroMode bool
	}{
		{"NO_ZERO_DATE", true, false, false},
		{"NO_ZERO_IN_DATE", false, true, false},
		{"ERROR_FOR_DIVISION_BY_ZERO", false, false, true},
		{"NO_ZERO_IN_DATE,NO_ZERO_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", true, true, true},
		{"NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", false, true, true},
		{"", false, false, false},
	}

	for _, tt := range tests {
		sqlMode, _ := mysql.GetSQLMode(tt.arg)
		if sqlMode.HasNoZeroDateMode() != tt.hasNoZeroDateMode {
			t.Fatalf("got %#v, want %#v", sqlMode.HasNoZeroDateMode(), tt.hasNoZeroDateMode)
		}
		if sqlMode.HasNoZeroInDateMode() != tt.hasNoZeroInDateMode {
			t.Fatalf("got %#v, want %#v", sqlMode.HasNoZeroInDateMode(), tt.hasNoZeroInDateMode)
		}
		if sqlMode.HasErrorForDivisionByZeroMode() != tt.hasErrorForDivisionByZeroMode {
			t.Fatalf("got %#v, want %#v", sqlMode.HasErrorForDivisionByZeroMode(), tt.hasErrorForDivisionByZeroMode)
		}
	}
}

func TestServerStatus(t *testing.T) {
	tests := []struct {
		arg            uint16
		IsCursorExists bool
	}{
		{0, false},
		{mysql.ServerStatusInTrans | mysql.ServerStatusNoBackslashEscaped, false},
		{mysql.ServerStatusCursorExists, true},
		{mysql.ServerStatusCursorExists | mysql.ServerStatusLastRowSend, true},
	}

	for _, tt := range tests {
		ret := mysql.HasCursorExistsFlag(tt.arg)
		if ret != tt.IsCursorExists {
			t.Fatalf("got %#v, want %#v", ret, tt.IsCursorExists)
		}
	}
}
