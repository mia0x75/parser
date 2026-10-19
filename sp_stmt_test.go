// Copyright 2024 PingCAP, Inc.
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
	"reflect"
	"strings"
	"testing"

	"github.com/mia0x75/parser/ast"
	fmtrestore "github.com/mia0x75/parser/format"
)

func mustParseOne(t *testing.T, sql string) ast.StmtNode {
	t.Helper()
	stmt, err := New().ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return stmt
}

func TestSpStmtParse(t *testing.T) {
	cases := []string{
		"create procedure p() begin select 1; end",
		"create procedure p() begin select 1; select 2; end",
		"create procedure p() lbl: begin declare a int default 0; set a = 1; end lbl",
		"create procedure p() lbl: begin select 1; end",
		"create procedure p() begin if 1 then select 1; elseif 2 then select 2; else select 3; end if; end",
		"create procedure p() begin while 1 do select 1; end while; end",
		"create procedure p() begin repeat select 1; until 1 end repeat; end",
		"create procedure p() begin loop select 1; end loop; end",
		"create procedure p() begin lbl: loop leave lbl; end loop lbl; end",
		"create procedure p() begin lbl: while 1 do iterate lbl; end while lbl; end",
		"create procedure p() begin case 1 when 1 then select 1; else select 2; end case; end",
		"create procedure p() begin case when 1 = 1 then select 1; end case; end",
		"create procedure p() begin declare c cursor for select 1; open c; fetch c into a, b; close c; end",
		"create procedure p() begin declare continue handler for not found select 1; end",
		"create procedure p() begin declare exit handler for sqlexception, sqlwarning select 1; end",
		"create procedure p() begin declare undo handler for 1062 begin select 1; end; end",
		"create procedure p() begin declare e condition for sqlstate '45000'; end",
		"create procedure p() begin declare e condition for 1062; end",
		"create procedure p() begin signal sqlstate '45000' set message_text = 'x'; end",
		"create procedure p() begin resignal; end",
		"create procedure p() begin resignal set message_text = 'x'; end",
		"create procedure p() begin resignal sqlstate '45000' set message_text = 'x'; end",
		"create procedure p() begin start transaction; commit; rollback; end",
		"create procedure p() begin select end from t; end",
		"create procedure p() begin insert into t values (1) on duplicate key update a = 1; end",
		"create procedure p() begin select 1 into a; end",
		"create procedure p() begin select a, b into x, y from t where a > 0; end",
		"create procedure p() begin if 1 then end if; end",
		"create procedure p() begin case when 1 then end case; end",
		"create procedure p() begin while 1 do end while; end",
		"create procedure p() begin repeat until 1 end repeat; end",
		"create procedure p() begin loop end loop; end",
		"create procedure p() begin not atomic select 1; end",
		"create function f() returns int return 1",
		"create function f() returns int begin return 1; end",
		"create trigger tr before insert on t for each row begin signal sqlstate '45000'; end",
		"create event e on schedule every 1 day do begin select 1; end",
	}
	for _, sql := range cases {
		if _, err := New().ParseOneStmt(sql, "", ""); err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
	}
}

func TestSpStmtParseError(t *testing.T) {
	cases := []struct {
		sql string
		msg string
	}{
		{"create procedure p() lbl: begin select 1; end other", "does not match"},
		{"create procedure p() lbl: loop select 1; end loop other", "does not match"},
		{"create procedure p() lbl: while 1 do select 1; end while other", "does not match"},
		{"create procedure p() lbl: repeat select 1; until 1 end repeat other", "does not match"},
		{"create procedure p() begin select 1 end", ""},
	}
	for _, c := range cases {
		_, err := New().ParseOneStmt(c.sql, "", "")
		if err == nil {
			t.Fatalf("expected error for %q", c.sql)
		}
		if c.msg != "" && !strings.Contains(err.Error(), c.msg) {
			t.Fatalf("error for %q = %q, want containing %q", c.sql, err.Error(), c.msg)
		}
	}
}

func restoreBody(t *testing.T, sql string) string {
	t.Helper()
	stmt, err := New().ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	var body ast.StmtNode
	switch s := stmt.(type) {
	case *ast.CreateProcedureStmt:
		body = s.Body
	case *ast.CreateFunctionStmt:
		body = s.Body
	case *ast.CreateTriggerStmt:
		body = s.Body
	case *ast.CreateEventStmt:
		body = s.Body
	default:
		t.Fatalf("unexpected stmt type %T", stmt)
	}
	var sb strings.Builder
	if err := body.Restore(fmtrestore.NewRestoreCtx(fmtrestore.DefaultRestoreFlags, &sb)); err != nil {
		t.Fatalf("restore %q: %v", sql, err)
	}
	return sb.String()
}

func TestSpStmtRestore(t *testing.T) {
	cases := []struct {
		sql   string
		wraps string
	}{
		{"create procedure p() begin select 1; end", "create procedure p() %s"},
		{"create procedure p() begin select 1; select 2; end", "create procedure p() %s"},
		{"create procedure p() lbl: begin declare a int default 0; set a = 1; end lbl", "create procedure p() %s"},
		{"create procedure p() begin if 1 then select 1; elseif 2 then select 2; else select 3; end if; end", "create procedure p() %s"},
		{"create procedure p() begin while 1 do select 1; end while; end", "create procedure p() %s"},
		{"create procedure p() begin repeat select 1; until 1 end repeat; end", "create procedure p() %s"},
		{"create procedure p() begin loop select 1; end loop; end", "create procedure p() %s"},
		{"create procedure p() begin lbl: loop leave lbl; end loop lbl; end", "create procedure p() %s"},
		{"create procedure p() begin case 1 when 1 then select 1; else select 2; end case; end", "create procedure p() %s"},
		{"create procedure p() begin case when 1 = 1 then select 1; end case; end", "create procedure p() %s"},
		{"create procedure p() begin declare c cursor for select 1; open c; fetch c into a, b; close c; end", "create procedure p() %s"},
		{"create procedure p() begin declare continue handler for not found select 1; end", "create procedure p() %s"},
		{"create procedure p() begin declare exit handler for sqlexception, sqlwarning select 1; end", "create procedure p() %s"},
		{"create procedure p() begin declare e condition for sqlstate '45000'; end", "create procedure p() %s"},
		{"create procedure p() begin signal sqlstate '45000' set message_text = 'x'; end", "create procedure p() %s"},
		{"create procedure p() begin resignal sqlstate '45000' set message_text = 'x'; end", "create procedure p() %s"},
		{"create procedure p() begin select a, b into x, y from t where a > 0; end", "create procedure p() %s"},
	}
	for _, c := range cases {
		orig := mustParseOne(t, c.sql)
		restored := restoreBody(t, c.sql)
		reparsed := mustParseOne(t, strings.Replace(c.wraps, "%s", restored, 1))
		ob := nodeBody(orig)
		rb := nodeBody(reparsed)
		CleanNodeText(ob)
		CleanNodeText(rb)
		if !reflect.DeepEqual(ob, rb) {
			t.Fatalf("restore mismatch for %q:\n restored: %s", c.sql, restored)
		}
	}
}

func nodeBody(stmt ast.StmtNode) ast.StmtNode {
	switch s := stmt.(type) {
	case *ast.CreateProcedureStmt:
		return s.Body
	case *ast.CreateFunctionStmt:
		return s.Body
	case *ast.CreateTriggerStmt:
		return s.Body
	case *ast.CreateEventStmt:
		return s.Body
	}
	return nil
}

func TestDelimiter(t *testing.T) {
	cases := []struct {
		sql string
		n   int
	}{
		{"delimiter $$\ncreate procedure p() select 1$$", 1},
		{"DELIMITER //\nselect 1//\nselect 2//", 2},
		{"select 1; select 2;", 2},
		{"delimiter $$\nselect 1$$\ndelimiter ;\nselect 2;", 2},
		{"delimiter $$\ncreate procedure p() begin select 1; select 2; end$$", 1},
		{"delimiter $$\ncreate procedure p() begin select 1; select 2; end$$\nselect 3;", 2},
	}
	for _, c := range cases {
		stmts, _, err := New().Parse(c.sql, "", "")
		if err != nil {
			t.Fatalf("sql %q: %v", c.sql, err)
		}
		if len(stmts) != c.n {
			t.Fatalf("sql %q: got %d stmts want %d", c.sql, len(stmts), c.n)
		}
	}
	// `delimiter` as a plain identifier is not treated as a command.
	if _, err := New().ParseOneStmt("select `delimiter` from t", "", ""); err != nil {
		t.Fatalf("identifier delimiter: %v", err)
	}
}
