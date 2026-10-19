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
	"github.com/mia0x75/parser/model"
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
		"select a from t group by a with cascaded check option",
		"select a from t group by a with local check option",
		"create view v as select a from t group by a with cascaded check option",
		"create view v as select a from t group by a with local check option",
		"alter view v as select a from t group by a with local check option",

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

		// Set operations.
		"select 1 union select 2",
		"select 1 union all select 2",
		"select 1 union distinct select 2",
		"select 1 intersect select 2",
		"select 1 intersect all select 2",
		"select 1 except select 2",
		"select 1 except all select 2",
		"select 1 union select 2 intersect select 3",
		"select 1 intersect select 2 union select 3",
		"select 1 except select 2 union select 3",
		"(select 1) union (select 2)",
		"select 1 union (select 2 order by 1)",
		"select 1 union select 2 order by 1 limit 1",
		"select 1 union (select 2) order by 1 limit 1",
		"(select 1 order by 1) union select 2",
		"with cte as (select 1) select * from cte union select 2",
		"insert into t select 1 union select 2",
		"create table t as select 1 intersect select 2",

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

		// ALTER TABLE partition / tablespace operations.
		"alter table t exchange partition p0 with table t2",
		"alter table t exchange partition p0 with table t2 without validation",
		"alter table t reorganize partition p0, p1 into (partition p0 values less than (10), partition p1 values less than (20))",
		"alter table t reorganize partition all into (partition p0 values less than (10))",
		"alter table t remove partitioning",
		"alter table t discard tablespace",
		"alter table t import tablespace",

		// SELECT ... INTO OUTFILE / DUMPFILE and trailing INTO.
		"select * from t into outfile 'file.txt'",
		"select * from t into outfile 'file.txt' fields terminated by ',' enclosed by '\"' lines terminated by '\n'",
		"select * from t into outfile 'file.txt' character set utf8mb4 fields terminated by ','",
		"select * from t into dumpfile 'file.bin'",
		"select a from t into @x, @y",
		"select a, b from t into x",
		"select a into @x from t",

		// CHECK / REPAIR / OPTIMIZE / CHECKSUM TABLE.
		"check table t",
		"check table t1, t2 quick",
		"check table t for upgrade",
		"check table t fast medium extended changed",
		"repair table t",
		"repair table t quick extended use_frm",
		"repair table t local",
		"optimize table t",
		"optimize local table t",
		"checksum table t",
		"checksum table t1, t2 quick",

		// Column VISIBLE / INVISIBLE.
		"create table t (a int invisible)",
		"create table t (a int visible, b varchar(10) not null invisible)",
		"alter table t modify column a int invisible",
		"alter table t change column a b int visible",

		// EXPLAIN FORMAT=JSON|TRADITIONAL|TREE.
		"explain format=json select * from t",
		"explain format=traditional select 1",
		"explain format=tree select 1",
		"explain format='json' select 1",
		"explain format=tree for connection 3",
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

func TestAlterTablePartitionAST(t *testing.T) {
	stmt := mustParseOne(t, "alter table t exchange partition p0 with table t2 without validation").(*ast.AlterTableStmt)
	spec := stmt.Specs[0]
	if spec.Tp != ast.AlterTableExchangePartition || spec.Name != "p0" {
		t.Fatalf("unexpected exchange spec: %+v", spec)
	}
	if spec.NewTable == nil || spec.NewTable.Name.O != "t2" || !spec.WithoutValidation {
		t.Fatalf("unexpected exchange spec: %+v", spec)
	}
	reorg := mustParseOne(t, "alter table t reorganize partition p0, p1 into (partition p0 values less than (10))").(*ast.AlterTableStmt).Specs[0]
	if reorg.Tp != ast.AlterTableReorganizePartition || len(reorg.PartitionNames) != 2 || len(reorg.PartDefinitions) != 1 {
		t.Fatalf("unexpected reorganize spec: %+v", reorg)
	}
	rm := mustParseOne(t, "alter table t remove partitioning").(*ast.AlterTableStmt).Specs[0]
	if rm.Tp != ast.AlterTableRemovePartitioning {
		t.Fatalf("unexpected remove spec: %+v", rm)
	}
	disc := mustParseOne(t, "alter table t discard tablespace").(*ast.AlterTableStmt).Specs[0]
	if disc.Tp != ast.AlterTableDiscardTablespace {
		t.Fatalf("unexpected discard spec: %+v", disc)
	}
	impt := mustParseOne(t, "alter table t import tablespace").(*ast.AlterTableStmt).Specs[0]
	if impt.Tp != ast.AlterTableImportTablespace {
		t.Fatalf("unexpected import spec: %+v", impt)
	}
}

func TestSelectIntoAST(t *testing.T) {
	sel := mustParseOne(t, "select a from t into outfile 'f.txt' character set utf8mb4 fields terminated by ','").(*ast.SelectStmt)
	if sel.Into == nil || sel.Into.Tp != ast.SelectIntoOutfile || sel.Into.FileName != "f.txt" || sel.Into.Charset != "utf8mb4" {
		t.Fatalf("unexpected Into: %+v", sel.Into)
	}
	if sel.Into.Fields == nil || sel.Into.Fields.Terminated != "," {
		t.Fatalf("unexpected Into.Fields: %+v", sel.Into.Fields)
	}
	dump := mustParseOne(t, "select a from t into dumpfile 'f.bin'").(*ast.SelectStmt)
	if dump.Into == nil || dump.Into.Tp != ast.SelectIntoDumpfile || dump.Into.FileName != "f.bin" {
		t.Fatalf("unexpected dump Into: %+v", dump.Into)
	}
	trailing := mustParseOne(t, "select a from t into @x, y").(*ast.SelectStmt)
	if len(trailing.IntoVars) != 2 || trailing.IntoVars[0] != "@x" || trailing.IntoVars[1] != "y" {
		t.Fatalf("unexpected trailing IntoVars: %+v", trailing.IntoVars)
	}
}

func TestTableMaintenanceAST(t *testing.T) {
	chk := mustParseOne(t, "check table t1, t2 quick").(*ast.TableMaintenanceStmt)
	if chk.Tp != ast.TableMaintenanceCheck || len(chk.Tables) != 2 || chk.Tables[0].Name.L != "t1" || len(chk.Options) != 1 || chk.Options[0] != "QUICK" {
		t.Fatalf("unexpected check stmt: %+v", chk)
	}
	opt := mustParseOne(t, "optimize local table t").(*ast.TableMaintenanceStmt)
	if opt.Tp != ast.TableMaintenanceOptimize || len(opt.Tables) != 1 || len(opt.Options) != 0 {
		t.Fatalf("unexpected optimize stmt: %+v", opt)
	}
	sum := mustParseOne(t, "checksum table t").(*ast.TableMaintenanceStmt)
	if sum.Tp != ast.TableMaintenanceChecksum || len(sum.Tables) != 1 {
		t.Fatalf("unexpected checksum stmt: %+v", sum)
	}
	rep := mustParseOne(t, "repair table t for upgrade").(*ast.TableMaintenanceStmt)
	if rep.Tp != ast.TableMaintenanceRepair || len(rep.Options) != 1 || rep.Options[0] != "FOR UPGRADE" {
		t.Fatalf("unexpected repair stmt: %+v", rep)
	}
}

func TestColumnVisibilityAST(t *testing.T) {
	ct := mustParseOne(t, "create table t (a int invisible)").(*ast.CreateTableStmt)
	opts := ct.Cols[0].Options
	if len(opts) != 1 || opts[0].Tp != ast.ColumnOptionInvisible {
		t.Fatalf("unexpected options: %+v", opts)
	}
	ctv := mustParseOne(t, "create table t (a int visible)").(*ast.CreateTableStmt)
	if got := ctv.Cols[0].Options[len(ctv.Cols[0].Options)-1].Tp; got != ast.ColumnOptionVisible {
		t.Fatalf("unexpected visible option: %v", got)
	}
}

func TestExplainFormatAST(t *testing.T) {
	tree := mustParseOne(t, "explain format=tree select 1").(*ast.ExplainStmt)
	if tree.Format != "tree" {
		t.Fatalf("unexpected format: %q", tree.Format)
	}
	tj := mustParseOne(t, "explain format=JSON select 1").(*ast.ExplainStmt)
	if tj.Format != "json" {
		t.Fatalf("unexpected format: %q", tj.Format)
	}
	forConn := mustParseOne(t, "explain format=traditional for connection 7").(*ast.ExplainForStmt)
	if forConn.Format != "traditional" || forConn.ConnectionID != 7 {
		t.Fatalf("unexpected explain for: %+v", forConn)
	}
}

func TestGroupByCheckOptionAST(t *testing.T) {
	local := mustParseOne(t, "select a from t group by a with local check option").(*ast.SelectStmt)
	if local.GroupBy == nil || !local.GroupBy.HasCheckOption || local.GroupBy.CheckOption != model.CheckOptionLocal {
		t.Fatalf("unexpected group by: %+v", local.GroupBy)
	}
	cascaded := mustParseOne(t, "select a from t group by a with cascaded check option").(*ast.SelectStmt)
	if cascaded.GroupBy == nil || !cascaded.GroupBy.HasCheckOption || cascaded.GroupBy.CheckOption != model.CheckOptionCascaded {
		t.Fatalf("unexpected group by: %+v", cascaded.GroupBy)
	}
	rollup := mustParseOne(t, "select a from t group by a with rollup").(*ast.SelectStmt)
	if rollup.GroupBy == nil || !rollup.GroupBy.WithRollup || rollup.GroupBy.HasCheckOption {
		t.Fatalf("unexpected group by: %+v", rollup.GroupBy)
	}
}

func TestSetOprAST(t *testing.T) {
	stmt := mustParseOne(t, "select 1 union all select 2 except select 3 intersect select 4").(*ast.SetOprStmt)
	if len(stmt.SelectList.Selects) != 4 {
		t.Fatalf("Selects len = %d, want 4", len(stmt.SelectList.Selects))
	}
	wantOps := []ast.SetOprType{ast.UnionAll, ast.Except, ast.Intersect}
	for i, want := range wantOps {
		s := stmt.SelectList.Selects[i+1].(*ast.SelectStmt)
		if s.AfterSetOperator == nil || *s.AfterSetOperator != want {
			t.Fatalf("Selects[%d].AfterSetOperator = %v, want %v", i+1, s.AfterSetOperator, want)
		}
	}
	if stmt.SelectList.Selects[0].(*ast.SelectStmt).AfterSetOperator != nil {
		t.Fatalf("first select should not have a set operator")
	}

	distinct := mustParseOne(t, "select 1 except select 2").(*ast.SetOprStmt)
	if op := distinct.SelectList.Selects[1].(*ast.SelectStmt).AfterSetOperator; op == nil || *op != ast.Except {
		t.Fatalf("EXCEPT = %v, want Except", op)
	}
	all := mustParseOne(t, "select 1 intersect all select 2").(*ast.SetOprStmt)
	if op := all.SelectList.Selects[1].(*ast.SelectStmt).AfterSetOperator; op == nil || *op != ast.IntersectAll {
		t.Fatalf("INTERSECT ALL = %v, want IntersectAll", op)
	}

	grouped := mustParseOne(t, "select 1 union (select 2 intersect select 3)").(*ast.SetOprStmt)
	group, ok := grouped.SelectList.Selects[1].(*ast.SetOprSelectList)
	if !ok {
		t.Fatalf("Selects[1] = %T, want *ast.SetOprSelectList", grouped.SelectList.Selects[1])
	}
	if group.AfterSetOperator == nil || *group.AfterSetOperator != ast.Union {
		t.Fatalf("group.AfterSetOperator = %v, want Union", group.AfterSetOperator)
	}
}
