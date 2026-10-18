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
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestKeywordConsistent(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	parserFilename := path.Join(path.Dir(filename), "parser.y")
	parserFile, err := os.Open(parserFilename)
	if err != nil {
		t.Fatalf("expected nil, got %#v", err)
	}
	data, err := ioutil.ReadAll(parserFile)
	if err != nil {
		t.Fatalf("expected nil, got %#v", err)
	}
	content := string(data)

	reservedKeywordStartMarker := "\t/* The following tokens belong to ReservedKeyword. */"
	unreservedKeywordStartMarker := "\t/* The following tokens belong to UnReservedKeyword. */"
	notKeywordTokenStartMarker := "\t/* The following tokens belong to NotKeywordToken. */"
	identTokenEndMarker := "%token\t<item>"

	reservedKeywords := extractKeywords(content, reservedKeywordStartMarker, unreservedKeywordStartMarker)

	unreservedKeywords := extractKeywords(content, unreservedKeywordStartMarker, notKeywordTokenStartMarker)
	notKeywordTokens := extractKeywords(content, notKeywordTokenStartMarker, identTokenEndMarker)
	for k, v := range aliases {
		if !(k != v) {
			t.Fatalf("expected true")
		}
		if tokenMap[k] != tokenMap[v] {
			t.Fatalf("got %#v, want %#v", tokenMap[k], tokenMap[v])
		}
	}
	keywordCount := len(reservedKeywords) + len(unreservedKeywords) + len(notKeywordTokens)
	if len(tokenMap)-len(aliases) != keywordCount-len(windowFuncTokenMap) {
		t.Fatalf("got %#v, want %#v", len(tokenMap)-len(aliases), keywordCount-len(windowFuncTokenMap))
	}

	unreservedCollectionDef := extractKeywordsFromCollectionDef(content, "\nUnReservedKeyword:")
	if !reflect.DeepEqual(unreservedKeywords, unreservedCollectionDef) {
		t.Fatalf("got %#v, want %#v", unreservedKeywords, unreservedCollectionDef)
	}

	notKeywordTokensCollectionDef := extractKeywordsFromCollectionDef(content, "\nNotKeywordToken:")
	if !reflect.DeepEqual(notKeywordTokens, notKeywordTokensCollectionDef) {
		t.Fatalf("got %#v, want %#v", notKeywordTokens, notKeywordTokensCollectionDef)
	}
}

func extractMiddle(str, startMarker, endMarker string) string {
	startIdx := strings.Index(str, startMarker)
	if startIdx == -1 {
		return ""
	}
	str = str[startIdx+len(startMarker):]
	endIdx := strings.Index(str, endMarker)
	if endIdx == -1 {
		return ""
	}
	return str[:endIdx]
}

func extractQuotedWords(strs []string) []string {
	var words []string
	for _, str := range strs {
		word := extractMiddle(str, "\"", "\"")
		if word == "" {
			continue
		}
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

func extractKeywords(content, startMarker, endMarker string) []string {
	keywordSection := extractMiddle(content, startMarker, endMarker)
	lines := strings.Split(keywordSection, "\n")
	return extractQuotedWords(lines)
}

func extractKeywordsFromCollectionDef(content, startMarker string) []string {
	keywordSection := extractMiddle(content, startMarker, "\n\n")
	words := strings.Split(keywordSection, "|")
	return extractQuotedWords(words)
}
