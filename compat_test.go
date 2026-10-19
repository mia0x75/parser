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
	"strings"
	"testing"

	"github.com/mia0x75/parser/ast"
	fmtrestore "github.com/mia0x75/parser/format"
)

func restoreStmt(t *testing.T, stmt ast.StmtNode) string {
	t.Helper()
	var sb strings.Builder
	if err := stmt.Restore(fmtrestore.NewRestoreCtx(fmtrestore.DefaultRestoreFlags, &sb)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	return sb.String()
}

// TestCompatRoundTrip makes sure the newly supported MySQL/MariaDB syntax can be
// parsed, restored, and parsed again.
func TestCompatRoundTrip(t *testing.T) {
	cases := []string{
		// SELECT lock clauses.
		"select * from t for update",
		"select * from t for share",
		"select * from t lock in share mode",
		"select * from t for update of t",
		"select * from t for share of t, t2 nowait",
		"select * from t for update skip locked",

		// ALTER TABLE RENAME COLUMN.
		"alter table t rename column a to b",

		// CREATE TEMPORARY TABLE.
		"create temporary table t (a int)",
		"create temporary table t like t2",

		// GROUP BY WITH ROLLUP / CUBE.
		"select a, sum(b) from t group by a with rollup",
		"select a, sum(b) from t group by a with cube",
		"select cube(a, b), rollup(c) from t",

		// SAVEPOINT family.
		"savepoint sp1",
		"rollback to savepoint sp1",
		"rollback to sp1",
		"rollback work to sp1",
		"release savepoint sp1",
		"rollback",

		// CHECK constraints retained in the AST.
		"create table t (a int check (a > 0), constraint c1 check (a < 10))",

		// Partition selection.
		"insert into t partition (p0) values (1)",
		"delete from t partition (p0) where a = 1",
		"update t partition (p0) set a = 1 where a = 2",
		"select a from t partition (p0)",

		// VALUES / TABLE statements.
		"values row(1, 2), row(3, 4)",
		"table t",

		// Sequences.
		"create sequence s",
		"create or replace temporary sequence if not exists s increment by 2 minvalue 1 maxvalue 100 start with 5 cache 10 cycle",
		"create sequence s no minvalue no maxvalue nocycle",
		"drop sequence if exists s, t",

		// User defined functions.
		"create function udf_a returns string soname 'liba.so'",
		"create aggregate function udf_b returns integer soname 'libb.so'",
		"drop function udf_a",

		// RETURNING.
		"insert into t (a) values (1) returning a, b",
		"delete from t where a = 1 returning a",
		"update t set a = 1 where b = 2 returning a",

		// XA transactions.
		"xa start 'x1'",
		"xa begin 'x1' join",
		"xa end 'x1' suspend for migrate",
		"xa prepare 'x1'",
		"xa commit 'x1' one phase",
		"xa rollback 'x1'",
		"xa recover",

		// SELECT ... INTO user variables.
		"select a, b into @x, @y from t",
		"select a into x from t",

		// Common table expressions.
		"with cte as (select 1) select * from cte",
		"with recursive cte (n) as (select 1 union all select n + 1 from cte where n < 5) select * from cte",
		"with a as (select 1), b as (select 2) select * from a, b",

		// Tablespaces.
		"create tablespace ts add datafile 'ts.ibd' initial_size 1048576 engine innodb",
		"create undo tablespace ts add datafile 'ts.ibd'",
		"create tablespace ts add datafile 'ts.ibd' file_block_size 16384 extent_size 1048576 autoextend_size 4194304 max_size 268435456 nodegroup 0 wait comment 'hello'",
		"alter tablespace ts add datafile 'ts2.ibd' no wait",
		"alter tablespace ts rename to ts2",
		"drop tablespace ts",
		"drop undo tablespace ts engine innodb",

		// Foreign servers.
		"create server srv foreign data wrapper 'mysql' options(host 'h', database 'd', user 'u', password 'p', port '3306')",
		"create server srv foreign data wrapper 'mysql'",
		"alter server srv options(user 'u2')",
		"drop server if exists srv",

		// Resource groups.
		"create resource group rg type = system vcpu = 0-3, 5 thread_priority 10 enable",
		"create resource group rg type = user vcpu = 1",
		"alter resource group rg vcpu = 2 thread_priority 5 disable",
		"drop resource group rg",
		"drop resource group rg force",

		// Logfile groups.
		"create logfile group lg add undofile 'undo.dat' initial_size 1048576 undo_buffer_size 8388608 redo_buffer_size 4194304 nodegroup 0 wait comment 'x' engine innodb",
		"alter logfile group lg add undofile 'undo2.dat' initial_size 2097152",
		"drop logfile group lg engine innodb",
	}
	for _, sql := range cases {
		stmt, err := New().ParseOneStmt(sql, "", "")
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		got := restoreStmt(t, stmt)
		if _, err := New().ParseOneStmt(got, "", ""); err != nil {
			t.Fatalf("reparse %q (from %q): %v", got, sql, err)
		}
	}
}

func TestSelectLockAST(t *testing.T) {
	stmt := mustParseOne(t, "select * from t for update of t skip locked")
	sel := stmt.(*ast.SelectStmt)
	if sel.LockTp != ast.SelectLockForUpdate {
		t.Fatalf("LockTp = %v, want SelectLockForUpdate", sel.LockTp)
	}
	if sel.LockWaitPolicy != ast.SelectLockWaitSkipLocked {
		t.Fatalf("LockWaitPolicy = %v, want SelectLockWaitSkipLocked", sel.LockWaitPolicy)
	}
	if len(sel.LockedTables) != 1 || sel.LockedTables[0].Name.O != "t" {
		t.Fatalf("LockedTables = %+v, want [t]", sel.LockedTables)
	}
}

func TestSavepointAST(t *testing.T) {
	sp := mustParseOne(t, "savepoint sp1").(*ast.SavepointStmt)
	if sp.Name != "sp1" {
		t.Fatalf("SavepointStmt.Name = %q, want sp1", sp.Name)
	}
	rb := mustParseOne(t, "rollback to savepoint sp1").(*ast.RollbackStmt)
	if rb.SavepointName != "sp1" {
		t.Fatalf("RollbackStmt.SavepointName = %q, want sp1", rb.SavepointName)
	}
	rs := mustParseOne(t, "release savepoint sp1").(*ast.ReleaseSavepointStmt)
	if rs.Name != "sp1" {
		t.Fatalf("ReleaseSavepointStmt.Name = %q, want sp1", rs.Name)
	}
}

func TestCheckConstraintRetained(t *testing.T) {
	stmt := mustParseOne(t, "create table t (a int check (a > 0), constraint c1 check (a < 10))").(*ast.CreateTableStmt)
	if len(stmt.Cols) != 1 || len(stmt.Cols[0].Options) != 1 || stmt.Cols[0].Options[0].Tp != ast.ColumnOptionCheck {
		t.Fatalf("column CHECK option not retained: %+v", stmt.Cols)
	}
	if len(stmt.Constraints) != 1 || stmt.Constraints[0].Tp != ast.ConstraintCheck || stmt.Constraints[0].Name != "c1" {
		t.Fatalf("table CHECK constraint not retained: %+v", stmt.Constraints)
	}
}

func TestReturningAST(t *testing.T) {
	ins := mustParseOne(t, "insert into t (a) values (1) returning a, b").(*ast.InsertStmt)
	if ins.Returning == nil || len(ins.Returning.Fields) != 2 {
		t.Fatalf("InsertStmt.Returning = %+v, want 2 fields", ins.Returning)
	}
	del := mustParseOne(t, "delete from t where a = 1 returning a").(*ast.DeleteStmt)
	if del.Returning == nil || len(del.Returning.Fields) != 1 {
		t.Fatalf("DeleteStmt.Returning = %+v, want 1 field", del.Returning)
	}
	upd := mustParseOne(t, "update t set a = 1 where b = 2 returning a").(*ast.UpdateStmt)
	if upd.Returning == nil || len(upd.Returning.Fields) != 1 {
		t.Fatalf("UpdateStmt.Returning = %+v, want 1 field", upd.Returning)
	}
}

func TestSequenceAST(t *testing.T) {
	stmt := mustParseOne(t, "create or replace sequence if not exists s increment by 2 minvalue 1").(*ast.CreateSequenceStmt)
	if !stmt.OrReplace || !stmt.IfNotExists || stmt.Name.Name.O != "s" {
		t.Fatalf("unexpected CreateSequenceStmt: %+v", stmt)
	}
	if len(stmt.Options) != 2 {
		t.Fatalf("CreateSequenceStmt.Options = %+v, want 2", stmt.Options)
	}
	drop := mustParseOne(t, "drop sequence if exists s, t").(*ast.DropSequenceStmt)
	if !drop.IfExists || len(drop.Names) != 2 {
		t.Fatalf("unexpected DropSequenceStmt: %+v", drop)
	}
}

func TestUDFAST(t *testing.T) {
	stmt := mustParseOne(t, "create aggregate function udf_b returns integer soname 'libb.so'").(*ast.CreateFunctionStmt)
	if !stmt.Aggregate || stmt.Soname != "libb.so" || stmt.Body != nil {
		t.Fatalf("unexpected UDF CreateFunctionStmt: %+v", stmt)
	}
}

func TestCTEAST(t *testing.T) {
	stmt := mustParseOne(t, "with recursive cte (n) as (select 1 union all select n + 1 from cte where n < 5) select * from cte").(*ast.SelectStmt)
	if stmt.With == nil || !stmt.With.IsRecursive || len(stmt.With.CTEs) != 1 {
		t.Fatalf("unexpected WithClause: %+v", stmt.With)
	}
	if stmt.With.CTEs[0].Name.O != "cte" || len(stmt.With.CTEs[0].ColNames) != 1 {
		t.Fatalf("unexpected CTE: %+v", stmt.With.CTEs[0])
	}
}

func TestXAStmtAST(t *testing.T) {
	stmt := mustParseOne(t, "xa commit 'x1' one phase").(*ast.XAStmt)
	if stmt.Command != "COMMIT" || stmt.XID != "x1" || stmt.Extra != "ONE PHASE" {
		t.Fatalf("unexpected XAStmt: %+v", stmt)
	}
}

func TestTablespaceAST(t *testing.T) {
	stmt := mustParseOne(t, "create undo tablespace ts add datafile 'ts.ibd' engine innodb").(*ast.CreateTablespaceStmt)
	if !stmt.Undo || stmt.Name != "ts" || len(stmt.Options) != 2 {
		t.Fatalf("unexpected CreateTablespaceStmt: %+v", stmt)
	}
	if stmt.Options[0].Tp != ast.TablespaceOptionDataFile || stmt.Options[0].Str != "ts.ibd" {
		t.Fatalf("unexpected datafile option: %+v", stmt.Options[0])
	}
	if stmt.Options[1].Tp != ast.TablespaceOptionEngine || stmt.Options[1].Str != "innodb" {
		t.Fatalf("unexpected engine option: %+v", stmt.Options[1])
	}
	alt := mustParseOne(t, "alter tablespace ts rename to ts2").(*ast.AlterTablespaceStmt)
	if alt.Name != "ts" || alt.RenameTo != "ts2" {
		t.Fatalf("unexpected AlterTablespaceStmt: %+v", alt)
	}
	drop := mustParseOne(t, "drop tablespace ts engine innodb").(*ast.DropTablespaceStmt)
	if drop.Name != "ts" || drop.Option == nil || drop.Option.Str != "innodb" {
		t.Fatalf("unexpected DropTablespaceStmt: %+v", drop)
	}
}

func TestServerAST(t *testing.T) {
	stmt := mustParseOne(t, "create server srv foreign data wrapper 'mysql' options(host 'h', user 'u')").(*ast.CreateServerStmt)
	if stmt.Name != "srv" || stmt.Wrapper != "mysql" || len(stmt.Options) != 2 {
		t.Fatalf("unexpected CreateServerStmt: %+v", stmt)
	}
	if stmt.Options[0].Name != "host" || stmt.Options[0].Value != "h" {
		t.Fatalf("unexpected server option: %+v", stmt.Options[0])
	}
	drop := mustParseOne(t, "drop server if exists srv").(*ast.DropServerStmt)
	if !drop.IfExists || drop.Name != "srv" {
		t.Fatalf("unexpected DropServerStmt: %+v", drop)
	}
}

func TestResourceGroupAST(t *testing.T) {
	stmt := mustParseOne(t, "create resource group rg type = system vcpu = 0-3, 5 thread_priority 10 enable").(*ast.CreateResourceGroupStmt)
	if stmt.Name != "rg" || !stmt.IsSystem || stmt.State != ast.ResourceGroupStateEnable {
		t.Fatalf("unexpected CreateResourceGroupStmt: %+v", stmt)
	}
	if len(stmt.VCPU) != 2 || stmt.VCPU[0].From != 0 || stmt.VCPU[0].To != 3 || stmt.VCPU[1].From != 5 || stmt.VCPU[1].To != 5 {
		t.Fatalf("unexpected VCPU: %+v", stmt.VCPU)
	}
	if stmt.ThreadPriority == nil || *stmt.ThreadPriority != 10 {
		t.Fatalf("unexpected ThreadPriority: %v", stmt.ThreadPriority)
	}
	drop := mustParseOne(t, "drop resource group rg force").(*ast.DropResourceGroupStmt)
	if drop.Name != "rg" || !drop.Force {
		t.Fatalf("unexpected DropResourceGroupStmt: %+v", drop)
	}
}

func TestLogfileGroupAST(t *testing.T) {
	stmt := mustParseOne(t, "create logfile group lg add undofile 'undo.dat' initial_size 1048576 undo_buffer_size 8388608").(*ast.CreateLogfileGroupStmt)
	if stmt.Name != "lg" || stmt.UndoFile != "undo.dat" || len(stmt.Options) != 2 {
		t.Fatalf("unexpected CreateLogfileGroupStmt: %+v", stmt)
	}
	if stmt.Options[0].Tp != ast.LogfileGroupOptionInitialSize || stmt.Options[0].Val != 1048576 {
		t.Fatalf("unexpected option: %+v", stmt.Options[0])
	}
	if stmt.Options[1].Tp != ast.LogfileGroupOptionUndoBufferSize || stmt.Options[1].Val != 8388608 {
		t.Fatalf("unexpected option: %+v", stmt.Options[1])
	}
}
