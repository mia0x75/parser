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

package charset

import (
	"math/rand"
	"reflect"
	"regexp"
	"testing"
)

func testValidCharset(t *testing.T, charset string, collation string, expect bool) {
	b := ValidCharsetAndCollation(charset, collation)
	if b != expect {
		t.Fatalf("got %#v, want %#v", b, expect)
	}
}

func TestValidCharset(t *testing.T) {
	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"utf8", "utf8_general_ci", true},
		{"", "utf8_general_ci", true},
		{"utf8mb4", "utf8mb4_bin", true},
		{"latin1", "latin1_bin", true},
		{"utf8", "utf8_invalid_ci", false},
		{"utf16", "utf16_bin", false},
		{"gb2312", "gb2312_chinese_ci", false},
		{"UTF8", "UTF8_BIN", true},
		{"UTF8", "utf8_bin", true},
		{"UTF8MB4", "utf8mb4_bin", true},
		{"UTF8MB4", "UTF8MB4_bin", true},
		{"UTF8MB4", "UTF8MB4_general_ci", true},
		{"Utf8", "uTf8_bIN", true},
	}
	for _, tt := range tests {
		testValidCharset(t, tt.cs, tt.co, tt.succ)
	}
}

func TestGetAllCharsets(t *testing.T) {
	charset := &Charset{"test", "test_bin", nil, "Test", 5}
	charsetInfos = append(charsetInfos, charset)
	descs := GetAllCharsets()
	if len(descs) != len(charsetInfos)-1 {
		t.Fatalf("got %#v, want %#v", len(descs), len(charsetInfos)-1)
	}
}

func testGetDefaultCollation(t *testing.T, charset string, expectCollation string, succ bool) {
	b, err := GetDefaultCollation(charset)
	if !succ {
		if err == nil {
			t.Fatal("got nil, want error")
		}
		return
	}
	if b != expectCollation {
		t.Fatalf("got %#v, want %#v", b, expectCollation)
	}
}

func TestGetDefaultCollation(t *testing.T) {
	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"utf8", "utf8_bin", true},
		{"UTF8", "utf8_bin", true},
		{"utf8mb4", "utf8mb4_bin", true},
		{"ascii", "ascii_bin", true},
		{"binary", "binary", true},
		{"latin1", "latin1_bin", true},
		{"invalid_cs", "", false},
		{"", "utf8_bin", false},
	}
	for _, tt := range tests {
		testGetDefaultCollation(t, tt.cs, tt.co, tt.succ)
	}
}

func TestGetCharsetDesc(t *testing.T) {
	tests := []struct {
		cs     string
		result string
		succ   bool
	}{
		{"utf8", "utf8", true},
		{"UTF8", "utf8", true},
		{"utf8mb4", "utf8mb4", true},
		{"ascii", "ascii", true},
		{"binary", "binary", true},
		{"latin1", "latin1", true},
		{"invalid_cs", "", false},
		{"", "utf8_bin", false},
	}
	for _, tt := range tests {
		desc, err := GetCharsetDesc(tt.cs)
		if !tt.succ {
			if err == nil {
				t.Fatal("got nil, want error")
			}
		} else {
			if desc.Name != tt.result {
				t.Fatalf("got %#v, want %#v", desc.Name, tt.result)
			}
		}
	}
}

func TestGetCollationByName(t *testing.T) {
	for _, collation := range collations {
		coll, err := GetCollationByName(collation.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(coll, collation) {
			t.Fatalf("got %#v, want %#v", coll, collation)
		}
	}

	_, err := GetCollationByName("non_exist")
	if err == nil || !regexp.MustCompile("\\[ddl:1273\\]Unknown collation: 'non_exist'").MatchString(err.Error()) {
		t.Fatalf("got %#v, want match for %#v", err, "\\[ddl:1273\\]Unknown collation: 'non_exist'")
	}
}

func BenchmarkGetCharsetDesc(b *testing.B) {
	b.ResetTimer()
	charsets := []string{CharsetUTF8, CharsetUTF8MB4, CharsetASCII, CharsetLatin1, CharsetBin}
	index := rand.Intn(len(charsets))
	cs := charsets[index]

	for i := 0; i < b.N; i++ {
		GetCharsetDesc(cs)
	}
}
