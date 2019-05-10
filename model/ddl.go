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
	"math"
	"time"
)

// ActionType is the type for DDL action.
type ActionType byte

// List DDL actions.
const (
	ActionNone                          ActionType = 0
	ActionCreateSchema                  ActionType = 1
	ActionDropSchema                    ActionType = 2
	ActionCreateTable                   ActionType = 3
	ActionDropTable                     ActionType = 4
	ActionAddColumn                     ActionType = 5
	ActionDropColumn                    ActionType = 6
	ActionAddIndex                      ActionType = 7
	ActionDropIndex                     ActionType = 8
	ActionAddForeignKey                 ActionType = 9
	ActionDropForeignKey                ActionType = 10
	ActionTruncateTable                 ActionType = 11
	ActionModifyColumn                  ActionType = 12
	ActionRebaseAutoID                  ActionType = 13
	ActionRenameTable                   ActionType = 14
	ActionSetDefaultValue               ActionType = 15
	ActionShardRowID                    ActionType = 16
	ActionModifyTableComment            ActionType = 17
	ActionRenameIndex                   ActionType = 18
	ActionAddTablePartition             ActionType = 19
	ActionDropTablePartition            ActionType = 20
	ActionCreateView                    ActionType = 21
	ActionModifyTableCharsetAndCollate  ActionType = 22
	ActionTruncateTablePartition        ActionType = 23
	ActionDropView                      ActionType = 24
	ActionRecoverTable                  ActionType = 25
	ActionModifySchemaCharsetAndCollate ActionType = 26
)

// AddIndexStr is a string related to the operation of "add index".
const AddIndexStr = "add index"

var actionMap = map[ActionType]string{
	ActionCreateSchema:                  "create schema",
	ActionDropSchema:                    "drop schema",
	ActionCreateTable:                   "create table",
	ActionDropTable:                     "drop table",
	ActionAddColumn:                     "add column",
	ActionDropColumn:                    "drop column",
	ActionAddIndex:                      AddIndexStr,
	ActionDropIndex:                     "drop index",
	ActionAddForeignKey:                 "add foreign key",
	ActionDropForeignKey:                "drop foreign key",
	ActionTruncateTable:                 "truncate table",
	ActionModifyColumn:                  "modify column",
	ActionRebaseAutoID:                  "rebase auto_increment ID",
	ActionRenameTable:                   "rename table",
	ActionSetDefaultValue:               "set default value",
	ActionShardRowID:                    "shard row ID",
	ActionModifyTableComment:            "modify table comment",
	ActionRenameIndex:                   "rename index",
	ActionAddTablePartition:             "add partition",
	ActionDropTablePartition:            "drop partition",
	ActionCreateView:                    "create view",
	ActionModifyTableCharsetAndCollate:  "modify table charset and collate",
	ActionTruncateTablePartition:        "truncate partition",
	ActionDropView:                      "drop view",
	ActionRecoverTable:                  "recover table",
	ActionModifySchemaCharsetAndCollate: "modify schema charset and collate",
}

// String return current ddl action in string
func (action ActionType) String() string {
	if v, ok := actionMap[action]; ok {
		return v
	}
	return "none"
}

// HistoryInfo is used for binlog.
type HistoryInfo struct {
	SchemaVersion int64
	DBInfo        *DBInfo
	TableInfo     *TableInfo
	FinishedTS    uint64
}

// AddDBInfo adds schema version and schema information that are used for binlog.
// dbInfo is added in the following operations: create database, drop database.
func (h *HistoryInfo) AddDBInfo(schemaVer int64, dbInfo *DBInfo) {
	h.SchemaVersion = schemaVer
	h.DBInfo = dbInfo
}

// AddTableInfo adds schema version and table information that are used for binlog.
// tblInfo is added except for the following operations: create database, drop database.
func (h *HistoryInfo) AddTableInfo(schemaVer int64, tblInfo *TableInfo) {
	h.SchemaVersion = schemaVer
	h.TableInfo = tblInfo
}

// Clean cleans history information.
func (h *HistoryInfo) Clean() {
	h.SchemaVersion = 0
	h.DBInfo = nil
	h.TableInfo = nil
}

// DDLReorgMeta is meta info of DDL reorganization.
type DDLReorgMeta struct {
	// EndHandle is the last handle of the adding indices table.
	// We should only backfill indices in the range [startHandle, EndHandle].
	EndHandle int64 `json:"end_handle"`
}

// NewDDLReorgMeta new a DDLReorgMeta.
func NewDDLReorgMeta() *DDLReorgMeta {
	return &DDLReorgMeta{
		EndHandle: math.MaxInt64,
	}
}

// SchemaDiff contains the schema modification at a particular schema version.
// It is used to reduce schema reload cost.
type SchemaDiff struct {
	Version  int64      `json:"version"`
	Type     ActionType `json:"type"`
	SchemaID int64      `json:"schema_id"`
	TableID  int64      `json:"table_id"`

	// OldTableID is the table ID before truncate, only used by truncate table DDL.
	OldTableID int64 `json:"old_table_id"`
	// OldSchemaID is the schema ID before rename table, only used by rename table DDL.
	OldSchemaID int64 `json:"old_schema_id"`
}

// TSConvert2Time converts timestamp to time.
func TSConvert2Time(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 is for the logical time.
	return time.Unix(t/1e3, (t%1e3)*1e6)
}
