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

package terror

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/mia0x75/parser/errors"
)

func TestErrCode(t *testing.T) {
	if CodeMissConnectionID != ErrCode(1) {
		t.Fatalf("got %#v, want %#v", CodeMissConnectionID, ErrCode(1))
	}
	if CodeResultUndetermined != ErrCode(2) {
		t.Fatalf("got %#v, want %#v", CodeResultUndetermined, ErrCode(2))
	}
}

func TestTError(t *testing.T) {
	if ClassParser.String() == "" {
		t.Fatal("ClassParser.String() is empty")
	}
	if ClassOptimizer.String() == "" {
		t.Fatal("ClassOptimizer.String() is empty")
	}
	if ClassKV.String() == "" {
		t.Fatal("ClassKV.String() is empty")
	}
	if ClassServer.String() == "" {
		t.Fatal("ClassServer.String() is empty")
	}

	parserErr := ClassParser.New(ErrCode(1), "error 1")
	if parserErr.Error() == "" {
		t.Fatal("parserErr.Error() is empty")
	}
	if !ClassParser.EqualClass(parserErr) {
		t.Fatal("ClassParser.EqualClass(parserErr) is false")
	}
	if ClassParser.NotEqualClass(parserErr) {
		t.Fatal("ClassParser.NotEqualClass(parserErr) is true")
	}

	if ClassOptimizer.EqualClass(parserErr) {
		t.Fatal("ClassOptimizer.EqualClass(parserErr) is true")
	}
	optimizerErr := ClassOptimizer.New(ErrCode(2), "abc")
	if ClassOptimizer.EqualClass(errors.New("abc")) {
		t.Fatal(`ClassOptimizer.EqualClass(errors.New("abc")) is true`)
	}
	if ClassOptimizer.EqualClass(nil) {
		t.Fatal("ClassOptimizer.EqualClass(nil) is true")
	}
	if !optimizerErr.Equal(optimizerErr.GenWithStack("def")) {
		t.Fatal("optimizerErr.Equal(optimizerErr.GenWithStack(def)) is false")
	}
	if optimizerErr.Equal(nil) {
		t.Fatal("optimizerErr.Equal(nil) is true")
	}
	if optimizerErr.Equal(errors.New("abc")) {
		t.Fatal(`optimizerErr.Equal(errors.New("abc")) is true`)
	}

	// Test case for FastGen.
	if !optimizerErr.Equal(optimizerErr.FastGen("def")) {
		t.Fatal("optimizerErr.Equal(optimizerErr.FastGen(def)) is false")
	}
	if !optimizerErr.Equal(optimizerErr.FastGen("def: %s", "def")) {
		t.Fatal("optimizerErr.Equal(FastGen with format args) is false")
	}
	kvErr := ClassKV.New(1062, "key already exist")
	e := kvErr.FastGen("Duplicate entry '%d' for key 'PRIMARY'", 1)
	if e.Error() != "[kv:1062]Duplicate entry '1' for key 'PRIMARY'" {
		t.Fatalf("got %#v, want %#v", e.Error(), "[kv:1062]Duplicate entry '1' for key 'PRIMARY'")
	}
	kvMySQLErrCodes := map[ErrCode]uint16{
		1062: uint16(1062),
	}
	ErrClassToMySQLCodes[ClassKV] = kvMySQLErrCodes
	sqlErr := errors.Cause(e).(*Error).ToSQLError()
	if sqlErr.Message != "Duplicate entry '1' for key 'PRIMARY'" {
		t.Fatalf("got %#v, want %#v", sqlErr.Message, "Duplicate entry '1' for key 'PRIMARY'")
	}
	if sqlErr.Code != uint16(1062) {
		t.Fatalf("got %#v, want %#v", sqlErr.Code, uint16(1062))
	}

	err := errors.Trace(ErrCritical.GenWithStackByArgs("test"))
	if !ErrCritical.Equal(err) {
		t.Fatal("ErrCritical.Equal(err) is false")
	}

	err = errors.Trace(ErrCritical)
	if !ErrCritical.Equal(err) {
		t.Fatal("ErrCritical.Equal(err) is false")
	}
}

func TestJson(t *testing.T) {
	prevTErr := &Error{
		class:   ClassTable,
		code:    CodeExecResultIsEmpty,
		message: "json test",
	}
	buf, err := json.Marshal(prevTErr)
	if err != nil {
		t.Fatal(err)
	}
	var curTErr Error
	err = json.Unmarshal(buf, &curTErr)
	if err != nil {
		t.Fatal(err)
	}
	isEqual := prevTErr.Equal(&curTErr)
	if !isEqual {
		t.Fatal("prevTErr.Equal(&curTErr) is false")
	}
}

var predefinedErr = ClassExecutor.New(ErrCode(123), "predefiend error")

func example() error {
	err := call()
	return errors.Trace(err)
}

func call() error {
	return predefinedErr.GenWithStack("error message:%s", "abc")
}

func TestTraceAndLocation(t *testing.T) {
	err := example()
	stack := errors.ErrorStack(err)
	lines := strings.Split(stack, "\n")
	var sysStack = 0
	for _, line := range lines {
		if strings.Contains(line, runtime.GOROOT()) {
			sysStack++
		}
	}
	if !(len(lines)-(2*sysStack) > 2) {
		t.Fatalf("got %#v, want > 2", len(lines)-(2*sysStack))
	}
	var containTerr bool
	for _, v := range lines {
		if strings.Contains(v, "terror_test.go") {
			containTerr = true
			break
		}
	}
	if !containTerr {
		t.Fatal("stack does not contain terror_test.go")
	}
}

func TestErrorEqual(t *testing.T) {
	e1 := errors.New("test error")
	if e1 == nil {
		t.Fatal("got nil, want non-nil")
	}

	e2 := errors.Trace(e1)
	if e2 == nil {
		t.Fatal("got nil, want non-nil")
	}

	e3 := errors.Trace(e2)
	if e3 == nil {
		t.Fatal("got nil, want non-nil")
	}

	if errors.Cause(e2) != e1 {
		t.Fatalf("got %#v, want %#v", errors.Cause(e2), e1)
	}
	if errors.Cause(e3) != e1 {
		t.Fatalf("got %#v, want %#v", errors.Cause(e3), e1)
	}
	if errors.Cause(e2) != errors.Cause(e3) {
		t.Fatalf("got %#v, want %#v", errors.Cause(e2), errors.Cause(e3))
	}

	e4 := errors.New("test error")
	if errors.Cause(e4) == e1 {
		t.Fatalf("got %#v, want not %#v", errors.Cause(e4), e1)
	}

	e5 := errors.Errorf("test error")
	if errors.Cause(e5) == e1 {
		t.Fatalf("got %#v, want not %#v", errors.Cause(e5), e1)
	}

	if !ErrorEqual(e1, e2) {
		t.Fatal("ErrorEqual(e1, e2) is false")
	}
	if !ErrorEqual(e1, e3) {
		t.Fatal("ErrorEqual(e1, e3) is false")
	}
	if !ErrorEqual(e1, e4) {
		t.Fatal("ErrorEqual(e1, e4) is false")
	}
	if !ErrorEqual(e1, e5) {
		t.Fatal("ErrorEqual(e1, e5) is false")
	}

	var e6 error

	if !ErrorEqual(nil, nil) {
		t.Fatal("ErrorEqual(nil, nil) is false")
	}
	if !ErrorNotEqual(e1, e6) {
		t.Fatal("ErrorNotEqual(e1, e6) is false")
	}
	code1 := ErrCode(1)
	code2 := ErrCode(2)
	te1 := ClassParser.New(code1, "abc")
	te2 := ClassParser.New(code1, "def")
	te3 := ClassKV.New(code1, "abc")
	te4 := ClassKV.New(code2, "abc")
	if !ErrorEqual(te1, te2) {
		t.Fatal("ErrorEqual(te1, te2) is false")
	}
	if ErrorEqual(te1, te3) {
		t.Fatal("ErrorEqual(te1, te3) is true")
	}
	if ErrorEqual(te3, te4) {
		t.Fatal("ErrorEqual(te3, te4) is true")
	}
}
