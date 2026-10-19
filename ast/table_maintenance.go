// Copyright 2023 PingCAP, Inc.
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

package ast

import (
	"github.com/mia0x75/parser/errors"
	"github.com/mia0x75/parser/format"
)

var _ StmtNode = &TableMaintenanceStmt{}

// TableMaintenanceType is the type of a table maintenance statement.
type TableMaintenanceType int

// Table maintenance statement types.
const (
	TableMaintenanceCheck TableMaintenanceType = iota + 1
	TableMaintenanceRepair
	TableMaintenanceOptimize
	TableMaintenanceChecksum
)

// TableMaintenanceStmt represents CHECK/REPAIR/OPTIMIZE/CHECKSUM TABLE.
type TableMaintenanceStmt struct {
	stmtNode

	Tp      TableMaintenanceType
	Tables  []*TableName
	Options []string
}

// Restore implements Node interface.
func (n *TableMaintenanceStmt) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case TableMaintenanceCheck:
		ctx.WriteKeyWord("CHECK TABLE ")
	case TableMaintenanceRepair:
		ctx.WriteKeyWord("REPAIR TABLE ")
	case TableMaintenanceOptimize:
		ctx.WriteKeyWord("OPTIMIZE TABLE ")
	case TableMaintenanceChecksum:
		ctx.WriteKeyWord("CHECKSUM TABLE ")
	default:
		return errors.New("An error occurred while splicing TableMaintenanceStmt")
	}
	for i, table := range n.Tables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore TableMaintenanceStmt.Tables[%d]", i)
		}
	}
	for _, opt := range n.Options {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(opt)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TableMaintenanceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableMaintenanceStmt)
	for i, table := range n.Tables {
		node, ok := table.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}
