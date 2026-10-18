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
	"testing"

	"github.com/mia0x75/parser/charset"
	"github.com/mia0x75/parser/mysql"
)

func TestFieldType(t *testing.T) {
	ft := NewFieldType(mysql.TypeDuration)
	if ft.Flen != UnspecifiedLength {
		t.Fatalf("got %#v, want %#v", ft.Flen, UnspecifiedLength)
	}
	if ft.Decimal != UnspecifiedLength {
		t.Fatalf("got %#v, want %#v", ft.Decimal, UnspecifiedLength)
	}
	ft.Decimal = 5
	if ft.String() != "time(5)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "time(5)")
	}

	ft = NewFieldType(mysql.TypeLong)
	ft.Flen = 5
	ft.Flag = mysql.UnsignedFlag | mysql.ZerofillFlag
	if ft.String() != "int(5) UNSIGNED ZEROFILL" {
		t.Fatalf("got %#v, want %#v", ft.String(), "int(5) UNSIGNED ZEROFILL")
	}
	if ft.InfoSchemaStr() != "int(5) unsigned" {
		t.Fatalf("got %#v, want %#v", ft.InfoSchemaStr(), "int(5) unsigned")
	}

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 12   // Default
	ft.Decimal = 3 // Not Default
	if ft.String() != "float(12,3)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "float(12,3)")
	}
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 12    // Default
	ft.Decimal = -1 // Default
	if ft.String() != "float" {
		t.Fatalf("got %#v, want %#v", ft.String(), "float")
	}
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	if ft.String() != "float" {
		t.Fatalf("got %#v, want %#v", ft.String(), "float")
	}
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	if ft.String() != "float(7,3)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "float(7,3)")
	}

	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 22   // Default
	ft.Decimal = 3 // Not Default
	if ft.String() != "double(22,3)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "double(22,3)")
	}
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 22    // Default
	ft.Decimal = -1 // Default
	if ft.String() != "double" {
		t.Fatalf("got %#v, want %#v", ft.String(), "double")
	}
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	if ft.String() != "double" {
		t.Fatalf("got %#v, want %#v", ft.String(), "double")
	}
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	if ft.String() != "double(7,3)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "double(7,3)")
	}

	ft = NewFieldType(mysql.TypeBlob)
	ft.Flen = 10
	ft.Charset = "UTF8"
	ft.Collate = "UTF8_UNICODE_GI"
	if ft.String() != "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI" {
		t.Fatalf("got %#v, want %#v", ft.String(), "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI")
	}

	ft = NewFieldType(mysql.TypeVarchar)
	ft.Flen = 10
	ft.Flag |= mysql.BinaryFlag
	if ft.String() != "varchar(10) BINARY" {
		t.Fatalf("got %#v, want %#v", ft.String(), "varchar(10) BINARY")
	}

	ft = NewFieldType(mysql.TypeString)
	ft.Charset = charset.CollationBin
	ft.Flag |= mysql.BinaryFlag
	if ft.String() != "binary(1)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "binary(1)")
	}

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b"}
	if ft.String() != "enum('a','b')" {
		t.Fatalf("got %#v, want %#v", ft.String(), "enum('a','b')")
	}

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"'a'", "'b'"}
	if ft.String() != "enum('''a''','''b''')" {
		t.Fatalf("got %#v, want %#v", ft.String(), "enum('''a''','''b''')")
	}

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a\tb", "a\rb"}
	if ft.String() != "enum('a\\nb','a\tb','a\\rb')" {
		t.Fatalf("got %#v", ft.String())
	}

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	if ft.String() != "enum('a\\nb','a''	\\r\\nb','a\\rb')" {
		t.Fatalf("got %#v", ft.String())
	}

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b"}
	if ft.String() != "set('a','b')" {
		t.Fatalf("got %#v, want %#v", ft.String(), "set('a','b')")
	}

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"'a'", "'b'"}
	if ft.String() != "set('''a''','''b''')" {
		t.Fatalf("got %#v, want %#v", ft.String(), "set('''a''','''b''')")
	}

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	if ft.String() != "set('a\\nb','a''	\\r\\nb','a\\rb')" {
		t.Fatalf("got %#v", ft.String())
	}

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a'\nb", "a'b\tc"}
	if ft.String() != "set('a''\\nb','a''b	c')" {
		t.Fatalf("got %#v", ft.String())
	}

	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 2
	if ft.String() != "timestamp(2)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "timestamp(2)")
	}
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 0
	if ft.String() != "timestamp" {
		t.Fatalf("got %#v, want %#v", ft.String(), "timestamp")
	}

	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 2
	if ft.String() != "datetime(2)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "datetime(2)")
	}
	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 0
	if ft.String() != "datetime" {
		t.Fatalf("got %#v, want %#v", ft.String(), "datetime")
	}

	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 2
	if ft.String() != "date" {
		t.Fatalf("got %#v, want %#v", ft.String(), "date")
	}
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 0
	if ft.String() != "date" {
		t.Fatalf("got %#v, want %#v", ft.String(), "date")
	}

	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 4
	ft.Decimal = 0
	if ft.String() != "year(4)" {
		t.Fatalf("got %#v, want %#v", ft.String(), "year(4)")
	}
	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 2
	ft.Decimal = 2
	if ft.String() != "year(2)" { // Note: Invalid year.
		t.Fatalf("got %#v, want %#v", ft.String(), "year(2)")
	}
}
