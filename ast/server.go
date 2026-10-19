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
	_ DDLNode = &CreateServerStmt{}
	_ DDLNode = &AlterServerStmt{}
	_ DDLNode = &DropServerStmt{}
	_ Node    = &ServerOption{}
)

// ServerOption represents a single option of a foreign server definition.
type ServerOption struct {
	node

	Name  string
	Value string
}

// Restore implements Node interface.
func (n *ServerOption) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain(n.Name)
	ctx.WritePlain(" ")
	ctx.WriteString(n.Value)
	return nil
}

// Accept implements Node Accept interface.
func (n *ServerOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ServerOption)
	return v.Leave(n)
}

// CreateServerStmt is a statement to create a foreign server.
// See https://dev.mysql.com/doc/refman/8.0/en/create-server.html
type CreateServerStmt struct {
	ddlNode

	Name    string
	Wrapper string
	Options []*ServerOption
}

// Restore implements Node interface.
func (n *CreateServerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE SERVER ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" FOREIGN DATA WRAPPER ")
	ctx.WriteString(n.Wrapper)
	if len(n.Options) > 0 {
		ctx.WriteKeyWord(" OPTIONS(")
		for i, opt := range n.Options {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			if err := opt.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while splicing CreateServerStmt")
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateServerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateServerStmt)
	for i, opt := range n.Options {
		node, ok := opt.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*ServerOption)
	}
	return v.Leave(n)
}

// AlterServerStmt is a statement to alter a foreign server.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-server.html
type AlterServerStmt struct {
	ddlNode

	Name    string
	Options []*ServerOption
}

// Restore implements Node interface.
func (n *AlterServerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER SERVER ")
	ctx.WriteName(n.Name)
	if len(n.Options) > 0 {
		ctx.WriteKeyWord(" OPTIONS(")
		for i, opt := range n.Options {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			if err := opt.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while splicing AlterServerStmt")
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterServerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterServerStmt)
	for i, opt := range n.Options {
		node, ok := opt.Accept(v)
		if !ok {
			return n, false
		}
		n.Options[i] = node.(*ServerOption)
	}
	return v.Leave(n)
}

// DropServerStmt is a statement to drop a foreign server.
// See https://dev.mysql.com/doc/refman/8.0/en/drop-server.html
type DropServerStmt struct {
	ddlNode

	IfExists bool
	Name     string
}

// Restore implements Node interface.
func (n *DropServerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SERVER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *DropServerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropServerStmt)
	return v.Leave(n)
}
