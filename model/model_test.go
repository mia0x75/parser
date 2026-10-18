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

package model

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/mia0x75/parser/mysql"
	"github.com/mia0x75/parser/types"
)

func TestT(t *testing.T) {
	abc := NewCIStr("aBC")
	if abc.O != "aBC" {
		t.Fatalf("got %#v, want %#v", abc.O, "aBC")
	}
	if abc.L != "abc" {
		t.Fatalf("got %#v, want %#v", abc.L, "abc")
	}
	if abc.String() != "aBC" {
		t.Fatalf("got %#v, want %#v", abc.String(), "aBC")
	}
}

func TestModelBasic(t *testing.T) {
	column := &ColumnInfo{
		ID:           1,
		Name:         NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
	}
	column.Flag |= mysql.PriKeyFlag

	index := &IndexInfo{
		Name:  NewCIStr("key"),
		Table: NewCIStr("t"),
		Columns: []*IndexColumn{
			{
				Name:   NewCIStr("c"),
				Offset: 0,
				Length: 10,
			}},
		Unique:  true,
		Primary: true,
	}

	fk := &FKInfo{
		RefCols: []CIStr{NewCIStr("a")},
		Cols:    []CIStr{NewCIStr("a")},
	}

	table := &TableInfo{
		ID:          1,
		Name:        NewCIStr("t"),
		Charset:     "utf8",
		Collate:     "utf8_bin",
		Columns:     []*ColumnInfo{column},
		Indices:     []*IndexInfo{index},
		ForeignKeys: []*FKInfo{fk},
		PKIsHandle:  true,
	}

	dbInfo := &DBInfo{
		ID:      1,
		Name:    NewCIStr("test"),
		Charset: "utf8",
		Collate: "utf8_bin",
		Tables:  []*TableInfo{table},
	}

	n := dbInfo.Clone()
	if !reflect.DeepEqual(n, dbInfo) {
		t.Fatalf("got %#v, want %#v", n, dbInfo)
	}

	pkName := table.GetPkName()
	if !reflect.DeepEqual(pkName, NewCIStr("c")) {
		t.Fatalf("got %#v, want %#v", pkName, NewCIStr("c"))
	}
	newColumn := table.GetPkColInfo()
	if !reflect.DeepEqual(newColumn, column) {
		t.Fatalf("got %#v, want %#v", newColumn, column)
	}
	inIdx := table.ColumnIsInIndex(column)
	if inIdx != true {
		t.Fatalf("got %#v, want %#v", inIdx, true)
	}
	tp := IndexTypeBtree
	if tp.String() != "BTREE" {
		t.Fatalf("got %#v, want %#v", tp.String(), "BTREE")
	}
	tp = IndexTypeHash
	if tp.String() != "HASH" {
		t.Fatalf("got %#v, want %#v", tp.String(), "HASH")
	}
	tp = 1e5
	if tp.String() != "" {
		t.Fatalf("got %#v, want %#v", tp.String(), "")
	}
	has := index.HasPrefixIndex()
	if has != true {
		t.Fatalf("got %#v, want %#v", has, true)
	}
	updateTime := table.GetUpdateTime()
	if !reflect.DeepEqual(updateTime, TSConvert2Time(table.UpdateTS)) {
		t.Fatalf("got %#v, want %#v", updateTime, TSConvert2Time(table.UpdateTS))
	}

	// Corner cases
	column.Flag ^= mysql.PriKeyFlag
	pkName = table.GetPkName()
	if !reflect.DeepEqual(pkName, NewCIStr("")) {
		t.Fatalf("got %#v, want %#v", pkName, NewCIStr(""))
	}
	newColumn = table.GetPkColInfo()
	if newColumn != nil {
		t.Fatalf("got %#v, want nil", newColumn)
	}
	anCol := &ColumnInfo{
		Name: NewCIStr("d"),
	}
	exIdx := table.ColumnIsInIndex(anCol)
	if exIdx != false {
		t.Fatalf("got %#v, want %#v", exIdx, false)
	}
	anIndex := &IndexInfo{
		Columns: []*IndexColumn{},
	}
	no := anIndex.HasPrefixIndex()
	if no != false {
		t.Fatalf("got %#v, want %#v", no, false)
	}
}

func TestString(t *testing.T) {
	acts := []struct {
		act    ActionType
		result string
	}{
		{ActionNone, "none"},
		{ActionAddForeignKey, "add foreign key"},
		{ActionDropForeignKey, "drop foreign key"},
		{ActionTruncateTable, "truncate table"},
		{ActionModifyColumn, "modify column"},
		{ActionRenameTable, "rename table"},
		{ActionSetDefaultValue, "set default value"},
		{ActionCreateSchema, "create schema"},
		{ActionDropSchema, "drop schema"},
		{ActionCreateTable, "create table"},
		{ActionDropTable, "drop table"},
		{ActionAddIndex, "add index"},
		{ActionDropIndex, "drop index"},
		{ActionAddColumn, "add column"},
		{ActionDropColumn, "drop column"},
		{ActionModifySchemaCharsetAndCollate, "modify schema charset and collate"},
	}

	for _, v := range acts {
		str := v.act.String()
		if str != v.result {
			t.Fatalf("got %#v, want %#v", str, v.result)
		}
	}
}

func TestUnmarshalCIStr(t *testing.T) {
	var ci CIStr

	// Test unmarshal CIStr from a single string.
	str := "aaBB"
	buf, err := json.Marshal(str)
	if err != nil {
		t.Fatal(err)
	}
	ci.UnmarshalJSON(buf)
	if ci.O != str {
		t.Fatalf("got %#v, want %#v", ci.O, str)
	}
	if ci.L != "aabb" {
		t.Fatalf("got %#v, want %#v", ci.L, "aabb")
	}

	buf, err = json.Marshal(ci)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != `{"O":"aaBB","L":"aabb"}` {
		t.Fatalf("got %#v, want %#v", string(buf), `{"O":"aaBB","L":"aabb"}`)
	}
	ci.UnmarshalJSON(buf)
	if ci.O != str {
		t.Fatalf("got %#v, want %#v", ci.O, str)
	}
	if ci.L != "aabb" {
		t.Fatalf("got %#v, want %#v", ci.L, "aabb")
	}
}
