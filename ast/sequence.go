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
	_ DDLNode = &CreateSequenceStmt{}
	_ DDLNode = &DropSequenceStmt{}
	_ Node    = &SequenceOption{}
)

// SequenceOptionType is the type for sequence options.
type SequenceOptionType int

// Sequence option types.
const (
	SequenceOptionNone SequenceOptionType = iota
	SequenceOptionIncrement
	SequenceOptionMinValue
	SequenceOptionNoMinValue
	SequenceOptionMaxValue
	SequenceOptionNoMaxValue
	SequenceOptionStart
	SequenceOptionCache
	SequenceOptionNoCache
	SequenceOptionCycle
	SequenceOptionNoCycle
)

// SequenceOption represents a single option of a sequence definition.
type SequenceOption struct {
	node

	Tp  SequenceOptionType
	Val int64
}

// Restore implements Node interface.
func (n *SequenceOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case SequenceOptionIncrement:
		ctx.WriteKeyWord("INCREMENT BY ")
		ctx.WritePlainf("%d", n.Val)
	case SequenceOptionMinValue:
		ctx.WriteKeyWord("MINVALUE ")
		ctx.WritePlainf("%d", n.Val)
	case SequenceOptionNoMinValue:
		ctx.WriteKeyWord("NO MINVALUE")
	case SequenceOptionMaxValue:
		ctx.WriteKeyWord("MAXVALUE ")
		ctx.WritePlainf("%d", n.Val)
	case SequenceOptionNoMaxValue:
		ctx.WriteKeyWord("NO MAXVALUE")
	case SequenceOptionStart:
		ctx.WriteKeyWord("START WITH ")
		ctx.WritePlainf("%d", n.Val)
	case SequenceOptionCache:
		ctx.WriteKeyWord("CACHE ")
		ctx.WritePlainf("%d", n.Val)
	case SequenceOptionNoCache:
		ctx.WriteKeyWord("NOCACHE")
	case SequenceOptionCycle:
		ctx.WriteKeyWord("CYCLE")
	case SequenceOptionNoCycle:
		ctx.WriteKeyWord("NO CYCLE")
	default:
		return errors.New("An error occurred while splicing SequenceOption")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SequenceOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SequenceOption)
	return v.Leave(n)
}

// CreateSequenceStmt is a statement to create a sequence (MariaDB).
// See https://mariadb.com/kb/en/create-sequence/
type CreateSequenceStmt struct {
	ddlNode

	IfNotExists bool
	OrReplace   bool
	Temporary   bool
	Name        *TableName
	Options     []*SequenceOption
}

// Restore implements Node interface.
func (n *CreateSequenceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	if n.Temporary {
		ctx.WriteKeyWord("TEMPORARY ")
	}
	ctx.WriteKeyWord("SEQUENCE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing CreateSequenceStmt Name")
	}
	for _, option := range n.Options {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateSequenceStmt Option")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateSequenceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateSequenceStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	for i, opt := range n.Options {
		node, ok := opt.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*SequenceOption)
	}
	return v.Leave(n)
}

// DropSequenceStmt is a statement to drop one or more sequences (MariaDB).
// See https://mariadb.com/kb/en/drop-sequence/
type DropSequenceStmt struct {
	ddlNode

	IfExists bool
	Names    []*TableName
}

// Restore implements Node interface.
func (n *DropSequenceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SEQUENCE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, name := range n.Names {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := name.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing DropSequenceStmt Names[%d]", i)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DropSequenceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropSequenceStmt)
	for i, name := range n.Names {
		node, ok := name.Accept(v)
		if !ok {
			return n, false
		}
		n.Names[i] = node.(*TableName)
	}
	return v.Leave(n)
}
