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

var (
	_ DDLNode = &CreateLogfileGroupStmt{}
	_ DDLNode = &AlterLogfileGroupStmt{}
	_ DDLNode = &DropLogfileGroupStmt{}
	_ Node    = &LogfileGroupOption{}
)

// LogfileGroupOptionType is the type for logfile group options.
type LogfileGroupOptionType int

// Logfile group option types.
const (
	LogfileGroupOptionNone LogfileGroupOptionType = iota
	LogfileGroupOptionInitialSize
	LogfileGroupOptionUndoBufferSize
	LogfileGroupOptionRedoBufferSize
	LogfileGroupOptionNodegroup
	LogfileGroupOptionWait
	LogfileGroupOptionComment
	LogfileGroupOptionEngine
)

// LogfileGroupOption represents a single option of a logfile group definition.
type LogfileGroupOption struct {
	node

	Tp  LogfileGroupOptionType
	Str string
	Val uint64
}

// Restore implements Node interface.
func (n *LogfileGroupOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case LogfileGroupOptionInitialSize:
		ctx.WriteKeyWord("INITIAL_SIZE = ")
		ctx.WritePlainf("%d", n.Val)
	case LogfileGroupOptionUndoBufferSize:
		ctx.WriteKeyWord("UNDO_BUFFER_SIZE = ")
		ctx.WritePlainf("%d", n.Val)
	case LogfileGroupOptionRedoBufferSize:
		ctx.WriteKeyWord("REDO_BUFFER_SIZE = ")
		ctx.WritePlainf("%d", n.Val)
	case LogfileGroupOptionNodegroup:
		ctx.WriteKeyWord("NODEGROUP = ")
		ctx.WritePlainf("%d", n.Val)
	case LogfileGroupOptionWait:
		ctx.WriteKeyWord("WAIT")
	case LogfileGroupOptionComment:
		ctx.WriteKeyWord("COMMENT = ")
		ctx.WriteString(n.Str)
	case LogfileGroupOptionEngine:
		ctx.WriteKeyWord("ENGINE = ")
		ctx.WritePlain(n.Str)
	default:
		return errors.New("An error occurred while splicing LogfileGroupOption")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *LogfileGroupOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LogfileGroupOption)
	return v.Leave(n)
}

func restoreLogfileGroupOptions(ctx *format.RestoreCtx, options []*LogfileGroupOption) error {
	for _, opt := range options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing LogfileGroupOptions")
		}
	}
	return nil
}

// CreateLogfileGroupStmt is a statement to create a logfile group.
// See https://dev.mysql.com/doc/refman/8.0/en/create-logfile-group.html
type CreateLogfileGroupStmt struct {
	ddlNode

	Name     string
	UndoFile string
	Options  []*LogfileGroupOption
}

// Restore implements Node interface.
func (n *CreateLogfileGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE LOGFILE GROUP ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" ADD UNDOFILE ")
	ctx.WriteString(n.UndoFile)
	return restoreLogfileGroupOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *CreateLogfileGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateLogfileGroupStmt)
	for i, opt := range n.Options {
		node, ok := opt.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*LogfileGroupOption)
	}
	return v.Leave(n)
}

// AlterLogfileGroupStmt is a statement to alter a logfile group.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-logfile-group.html
type AlterLogfileGroupStmt struct {
	ddlNode

	Name     string
	UndoFile string
	Options  []*LogfileGroupOption
}

// Restore implements Node interface.
func (n *AlterLogfileGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER LOGFILE GROUP ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" ADD UNDOFILE ")
	ctx.WriteString(n.UndoFile)
	return restoreLogfileGroupOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *AlterLogfileGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterLogfileGroupStmt)
	for i, opt := range n.Options {
		node, ok := opt.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*LogfileGroupOption)
	}
	return v.Leave(n)
}

// DropLogfileGroupStmt is a statement to drop a logfile group.
// See https://dev.mysql.com/doc/refman/8.0/en/drop-logfile-group.html
type DropLogfileGroupStmt struct {
	ddlNode

	Name    string
	Options []*LogfileGroupOption
}

// Restore implements Node interface.
func (n *DropLogfileGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP LOGFILE GROUP ")
	ctx.WriteName(n.Name)
	return restoreLogfileGroupOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *DropLogfileGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropLogfileGroupStmt)
	for i, opt := range n.Options {
		node, ok := opt.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*LogfileGroupOption)
	}
	return v.Leave(n)
}
