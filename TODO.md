# TODO

Local backlog for this fork. Each item records the problem, evidence, impact,
options and a recommendation so it can be decided later.

---

## 1. 函数名大小写导致 restore 往返 `DeepEqual` 不成立

**状态**: 待处理（暂无计划）
**来源**: 上游 pingcap/tidb parser 既有行为，并非本 fork 引入
**优先级**: 低（不影响 SQL 语义；主要影响测试与 AST 比对）

### 现象

`Parse → Restore → Parse` 得到的 AST 与原 AST 在 `reflect.DeepEqual` 下不相等，
只要函数名原文不是全大写就会发生。

### 根因

- `ast/functions.go:334`：`FuncCallExpr.Restore` 使用
  `ctx.WriteKeyWord(n.FnName.O)`。
- `format/format.go:334`：`WriteKeyWord` 依据 flags 大小写化：
  `DefaultRestoreFlags = RestoreStringSingleQuotes | RestoreKeyWordUppercase | RestoreNameBackQuotes`
  （`format/format.go:234`），即默认把关键字/函数名**转大写**。
- 但 AST 中 `FuncCallExpr.FnName`（`model.CIStr`）保留原始拼写：
  `O` 是原始大小写，`L` 是小写形式。restore 后重解析得到的是全大写 `O`，
  于是 `O` 字段不一致 → `DeepEqual` 为 false。

该 `WriteKeyWord` 行为来自上游（`git blame` 显示 `ast/functions.go:334`
为 Han Liu 2019-01-03 提交），本 fork 仅改过该文件的 import。

### 实测证据

| 输入 | `Text()`（原文） | restore | 重解析后 `O` |
|---|---|---|---|
| `select now()` | `select now()` | `SELECT NOW()` | `now` → `NOW` |
| `select myFunc(a)` | `select myFunc(a)` | ``SELECT MYFUNC(`a`)`` | `myFunc` → `MYFUNC` |
| `select date_add(d, interval 1 day)` | 同左 | ``SELECT DATE_ADD(`d`, INTERVAL 1 DAY)`` | `date_add` → `DATE_ADD` |
| `create event … at now() …` | 同左 | `… AT NOW() …` | `now` → `NOW` |

### 影响

1. **测试**：`RunRestoreTest` / `ast.RunNodeRestoreTest` 内部用
   `reflect.DeepEqual(restoreStmt, stmt)`（`parser_test.go:456`），
   凡函数名非全大写的用例都会失败。当前规避方式：对这类用例改为
   “只比 restore 字符串”，见 `TestRoutineRestoreString`
   （`parser_test.go`，含 `at now()` / `now()+interval` 等）。
2. **AST 等价/去重**：`SELECT now()` 与 `SELECT NOW()` 生成不同 AST
   （`O` 不同），语义相同却被判为不同；基于 AST 的缓存/比对会误判。
3. **往返/美化输出**：函数名大小写被归一化（`myFunc()` → `MYFUNC()`）。
   对 MySQL/MariaDB 语义无害（内置函数名、存储例程名大小写不敏感），
   但对期望“原样保留”的工具会造成意外。
4. **幂等性**：只有第一次 restore 会变化；`restore → parse → restore`
   之后稳定不变。

### 处理选项

1. **仅测试规避（现状）**：用 `Text()` 或只比 restore 字符串。
   改动最小，不影响对外行为。
2. **保真输出**：`FuncCallExpr.Restore` 改用 `WriteName`（或直接写
   `n.FnName.O`）。保留原始大小写，但会引入反引号、偏离上游默认输出，
   并需要更新大量既有期望 restore 串。
3. **解析时规范化**：parse 时把 `FnName.O` 直接置为大写，使往返稳定；
   代价是彻底丢失“原文大小写”信息。
4. **自定义比较**：给测试提供一个忽略函数名大小写的比较函数。

### 建议

- 若目标是与上游行为一致、且 `Text()` 已能保留原文 → 维持选项 1，不改代码。
- 若下游确实依赖“restore 保真”，再单独评估选项 2/3，并对全仓库
  restore 期望串做一次统一回归。**不要与本 fork 的语法扩展混在同一提交里。**

### 相关位置

- `ast/functions.go:334`（`FuncCallExpr.Restore`）
- `format/format.go:234`（`DefaultRestoreFlags`）
- `format/format.go:305`（`WriteKeyWord`）
- `parser_test.go:456`（`RunRestoreTest` 的 `DeepEqual`）
- `ast/util_test.go:114`（`RunNodeRestoreTest`）

---

## 2. 存储程序复合语句（compound statements）已实现及已知限制

**状态**: 已实现（v0.1.0）；下列为尚存的兼容性缺口
**来源**: 本 fork 新增（`ast/sp_stmt.go` + `parser.y` 的 `SpStmt` 语法）

已支持：`BEGIN...END`、标签、`DECLARE`（变量/条件/游标/处理器）、
`IF/ELSEIF/ELSE`、`CASE`（含与不含操作数）、`WHILE`、`REPEAT`、`LOOP`、
`LEAVE`、`ITERATE`、`OPEN/CLOSE/FETCH`、`SIGNAL/RESIGNAL`、
`START TRANSACTION`、`NOT ATOMIC`（MariaDB）、`SELECT ... INTO`、以及词法层
`DELIMITER`。复现用例见 `sp_stmt_test.go`。

### 已知限制

1. **复合块内语句列表要求以 `;` 结尾**（含块终止符前的最后一条）。
   MySQL/MariaDB 允许省略最后一条的 `;`（如 `BEGIN SET a=1 END`）。
   **根因**: `END` 需保持 UnReservedKeyword 以兼容 `SELECT end FROM tbl`
   （见 `parser_test.go:85`）；允许省略 `;` 会让 `end`/`until`/`elseif`
   等终止关键字既可作块结束又可作别名/标识符，从而产生 shift/reduce 冲突
   （`make parser` 拒绝任何冲突）。折中方案是强制 `;`，代价是该偏差。
   **选项**: 将 `END` 设为保留字（破坏既有用例）或接受现状。

2. **`SELECT ... INTO var_list` 仅支持 INTO 位于字段列表之后、FROM 之前**
   （含无 FROM 的情形），例如 `SELECT a INTO x FROM t`、`SELECT 1 INTO x`。
   **不支持** 末尾形式 `SELECT a FROM t INTO x`（MySQL 允许）。
   **根因**: 末尾 INTO 与「FROM 前 INTO」共享同一 lookahead，会引入
   shift/reduce 冲突（`SelectStmtBasic` 的 FOLLOW 含 `into`）。

3. **不支持 `GET DIAGNOSTICS`**（及其 `CONDITION`/`STATEMENT` 形式）。

4. **`INTO`/`FETCH ... INTO` 的变量支持普通标识符与用户变量 `@x`**；
   仍不支持系统变量 `@@xxx`。

5. **`DELIMITER` 只在词法层处理**：识别为「行首 `DELIMITER` + 空白 + 新分隔符」，
   并把自定义分隔符统一翻译成 `;` token。因此 `DELIMITER` 不是 AST 语句，
   也要求分隔符与语句间有空白（如 `end $$` 而非依赖 `$$` 与 `end` 相邻的
   极端场景；后者已通过 `incIdentChars` 兼容常见写法）。

### 相关位置

- `ast/sp_stmt.go`（全部复合语句 AST 节点）
- `parser.y`（`SpStmt`、`SpStmtList`、`SpBeginStmt` 及各类 `Sp*` 规则）
- `lexer.go`（`delimiter`/`stmtStart`/`consumeDelimiterCommand`）
- `sp_stmt_test.go`（解析 / 还原 / 分隔符用例）

---

## 3. 明确暂不处理的语法（Deferred）

**状态**: 暂不处理（明确搁置，非本 fork 目标）
**来源**: 兼容性盘点后按优先级排除
**说明**: 以下语法当前 `parser.y` 无对应规则，解析会报语法错误。经确认，
本阶段不实现，记录于此以免后续重复评估。如将来需要，请单独立项。

### 3.1 管理与复制（Administration / Replication）

完全搁置，包括但不限于：

- `START SLAVE` / `STOP SLAVE` / `START REPLICA` / `STOP REPLICA`
- `CHANGE MASTER TO` / `CHANGE REPLICATION SOURCE TO`
- `RESET MASTER` / `RESET SLAVE`（`SHOW MASTER STATUS` 已支持）
- `RESET`、`SHUTDOWN`
- `HANDLER tbl OPEN/READ/CLOSE` 语句
- `ADMIN ...`（MariaDB，如 `ADMIN SHUTDOWN/RELOAD/FLUSH ...`）
- `CACHE INDEX`、`LOAD INDEX INTO CACHE`、`LOAD XML`
- `RENAME USER`

### 3.3 持久化变量

- `SET PERSIST` / `SET PERSIST_ONLY`（MySQL 8.0）

### 3.5 ALTER TABLE 的分区 / 表空间操作

- `ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE ...`
- `ALTER TABLE ... REORGANIZE PARTITION ...`
- `ALTER TABLE ... REMOVE PARTITIONING`
- `ALTER TABLE ... DISCARD TABLESPACE` / `IMPORT TABLESPACE`

（注意：`ADD/DROP/TRUNCATE/COALESCE PARTITION` 已支持。）

### 3.6 IMPORT / 插件 / 克隆

- `IMPORT TABLE ...`
- `INSTALL PLUGIN` / `UNINSTALL PLUGIN`
- `INSTALL COMPONENT` / `UNINSTALL COMPONENT`
- `CLONE ...`

### 3.7 SELECT ... INTO 文件与末尾形式

- `SELECT ... INTO OUTFILE '...' ...` / `INTO DUMPFILE '...'`
- 末尾形式 `SELECT ... FROM t INTO var_list`
  （「字段列表之后、FROM 之前」的 `SELECT ... INTO var_list` 已支持，
  见第 2 节限制 2。）

### 3.8 LATERAL 与 JSON_TABLE

- `LATERAL` 派生表（MySQL 8.0.14）
- `JSON_TABLE(...)`（MySQL 8.0.4）

### 3.9 表维护语句

- `CHECK TABLE` / `REPAIR TABLE` / `OPTIMIZE TABLE` / `CHECKSUM TABLE`
  （仅 `ANALYZE TABLE` 已支持）

### 3.10 列可见性

- 列 `VISIBLE` / `INVISIBLE`（MySQL 8.0.23）

### 3.11 EXPLAIN 的输出格式

- `EXPLAIN FORMAT=JSON|TRADITIONAL|TREE SELECT ...`
  （目前 `FORMAT` 仅支持 `EXPLAIN FORMAT=... FOR CONNECTION N`）

### 3.12 GET DIAGNOSTICS

- `GET [CURRENT|STACKED] DIAGNOSTICS ...`
- 含 `CONDITION` / `STATEMENT` 形式

### 3.13 全文检索

- `MATCH (col, ...) AGAINST ('expr' [IN NATURAL LANGUAGE MODE | IN BOOLEAN MODE | WITH QUERY EXPANSION])`

### 3.14 JSON 谓词

- `MEMBER OF`
- `IS [NOT] JSON`（MySQL 8.0）

### 3.15 SOUNDS LIKE

- `expr SOUNDS LIKE expr`

---

## 4. v0.1.0 新增语法与遗留缺口

**状态**: 已实现（v0.1.0）；下列为仍需处理的问题

v0.1.0 已实现（用例见 `compat_test.go`）：

- SELECT 锁定子句：`FOR SHARE`、`FOR UPDATE OF t`、`NOWAIT`、`SKIP LOCKED`
- `ALTER TABLE ... RENAME COLUMN a TO b`
- `CREATE [OR REPLACE] TEMPORARY TABLE`
- `GROUP BY ... WITH ROLLUP` / `WITH CUBE`
- `SAVEPOINT` / `ROLLBACK TO [SAVEPOINT]` / `RELEASE SAVEPOINT`
- 表级/列级 `CHECK` 约束保留进 AST（此前被丢弃）
- 分区选择：`SELECT/INSERT/UPDATE/DELETE ... PARTITION (p0)`
- `VALUES ROW(...), ...` 与 `TABLE t` 语句
- `CREATE/DROP SEQUENCE`（MariaDB）
- UDF：`CREATE [AGGREGATE] FUNCTION ... RETURNS type SONAME 'lib'`
- `INSERT/UPDATE/DELETE ... RETURNING`
- `XA START|BEGIN|END|PREPARE|COMMIT|ROLLBACK|RECOVER`
- `SELECT ... INTO @user_var` / `FETCH ... INTO @user_var`
- CTE：`WITH [RECURSIVE] name [(cols)] AS (subquery) SELECT ...`

### 4.1 已知回归：`GROUP BY` + `WITH CASCADED|LOCAL CHECK OPTION`

引入 `GROUP BY ... WITH ROLLUP/CUBE` 后，`WITH` 被赋予了新的优先级
（`%precedence with`，见 `parser.y`），导致视图定义中紧随 `GROUP BY` 的
`WITH CASCADED CHECK OPTION` 无法解析：

```
create view v as select a from t group by a with cascaded check option
-- error: line 1 column 57 near "cascaded check option"
```

**影响面窄**：仅当 `CREATE/ALTER VIEW` 的查询同时含 `GROUP BY` 且直接跟
`WITH CASCADED|LOCAL CHECK OPTION` 时触发。（无 `GROUP BY` 的
`... WITH CASCADED CHECK OPTION` 仍可解析；注意纯 `WITH CHECK OPTION`
本就不在语法内。）

**建议**：后续修复 `WITH` 的歧义（例如仅在 `GROUP BY` 的 FOLLOW 中允许
`WITH ROLLUP/CUBE`，把 `CHECK OPTION` 提升到视图层处理），修复前先保留现状。

### 4.2 暂不支持的集合运算

- `INTERSECT [ALL|DISTINCT]`、`EXCEPT [ALL|DISTINCT]`
  （目前仅支持 `UNION [ALL|DISTINCT]`；MariaDB 支持，且 `INTERSECT`
  优先级高于 `UNION`/`EXCEPT`，需为 `UnionStmt` 引入运算符与优先级处理）

### 4.3 暂不支持的 GROUP BY 扩展

- `GROUPING SETS ((...), ...)`
  （`GROUPING(expr)` 函数、`CUBE(...)`、`ROLLUP(...)`、`WITH CUBE/ROLLUP`
  已支持）


---

## 5. v0.2.0 新增语法

**状态**: 已实现（v0.2.0，分多次提交）；用例见 `compat_test.go`

逐项完成原第 3 节中搁置的语法：

- 3.2 表空间 / 服务器对象：`CREATE/ALTER/DROP [UNDO] TABLESPACE`、
  `CREATE/ALTER/DROP SERVER`（`FOREIGN DATA WRAPPER ... OPTIONS(...)`）
- 3.4 资源组 / 日志组：`CREATE/ALTER/DROP RESOURCE GROUP`（`TYPE`、
  `VCPU`、`THREAD_PRIORITY`、`ENABLE/DISABLE`、`DROP ... FORCE`）、
  `CREATE/ALTER/DROP LOGFILE GROUP`
