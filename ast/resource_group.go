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
	"github.com/mia0x75/parser/format"
)

var (
	_ DDLNode = &CreateResourceGroupStmt{}
	_ DDLNode = &AlterResourceGroupStmt{}
	_ DDLNode = &DropResourceGroupStmt{}
	_ Node    = &ResourceGroupVCPU{}
)

// Resource group states.
const (
	ResourceGroupStateNone = iota
	ResourceGroupStateEnable
	ResourceGroupStateDisable
)

// ResourceGroupVCPU is a single VCPU range in a resource group definition.
type ResourceGroupVCPU struct {
	node

	From uint64
	To   uint64
}

// Restore implements Node interface.
func (n *ResourceGroupVCPU) Restore(ctx *format.RestoreCtx) error {
	if n.From == n.To {
		ctx.WritePlainf("%d", n.From)
	} else {
		ctx.WritePlainf("%d-%d", n.From, n.To)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ResourceGroupVCPU) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ResourceGroupVCPU)
	return v.Leave(n)
}

func restoreVCPUList(ctx *format.RestoreCtx, vcpu []*ResourceGroupVCPU) error {
	ctx.WriteKeyWord(" VCPU = ")
	for i, v := range vcpu {
		if i > 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return err
		}
	}
	return nil
}

func restoreResourceGroupOptional(ctx *format.RestoreCtx, tp *int, state int) {
	if tp != nil {
		ctx.WriteKeyWord(" THREAD_PRIORITY = ")
		ctx.WritePlainf("%d", *tp)
	}
	switch state {
	case ResourceGroupStateEnable:
		ctx.WriteKeyWord(" ENABLE")
	case ResourceGroupStateDisable:
		ctx.WriteKeyWord(" DISABLE")
	}
}

// CreateResourceGroupStmt is a statement to create a resource group.
// See https://dev.mysql.com/doc/refman/8.0/en/create-resource-group.html
type CreateResourceGroupStmt struct {
	ddlNode

	Name           string
	IsSystem       bool
	VCPU           []*ResourceGroupVCPU
	ThreadPriority *int
	State          int
}

// Restore implements Node interface.
func (n *CreateResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE RESOURCE GROUP ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" TYPE = ")
	if n.IsSystem {
		ctx.WriteKeyWord("SYSTEM")
	} else {
		ctx.WriteKeyWord("USER")
	}
	if len(n.VCPU) > 0 {
		if err := restoreVCPUList(ctx, n.VCPU); err != nil {
			return err
		}
	}
	restoreResourceGroupOptional(ctx, n.ThreadPriority, n.State)
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateResourceGroupStmt)
	for i, vcpu := range n.VCPU {
		node, ok := vcpu.Accept(v)
		if !ok {
			return n, false
		}
		n.VCPU[i] = node.(*ResourceGroupVCPU)
	}
	return v.Leave(n)
}

// AlterResourceGroupStmt is a statement to alter a resource group.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-resource-group.html
type AlterResourceGroupStmt struct {
	ddlNode

	Name           string
	VCPU           []*ResourceGroupVCPU
	ThreadPriority *int
	State          int
}

// Restore implements Node interface.
func (n *AlterResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER RESOURCE GROUP ")
	ctx.WriteName(n.Name)
	if len(n.VCPU) > 0 {
		if err := restoreVCPUList(ctx, n.VCPU); err != nil {
			return err
		}
	}
	restoreResourceGroupOptional(ctx, n.ThreadPriority, n.State)
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterResourceGroupStmt)
	for i, vcpu := range n.VCPU {
		node, ok := vcpu.Accept(v)
		if !ok {
			return n, false
		}
		n.VCPU[i] = node.(*ResourceGroupVCPU)
	}
	return v.Leave(n)
}

// DropResourceGroupStmt is a statement to drop a resource group.
// See https://dev.mysql.com/doc/refman/8.0/en/drop-resource-group.html
type DropResourceGroupStmt struct {
	ddlNode

	Name  string
	Force bool
}

// Restore implements Node interface.
func (n *DropResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP RESOURCE GROUP ")
	ctx.WriteName(n.Name)
	if n.Force {
		ctx.WriteKeyWord(" FORCE")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DropResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropResourceGroupStmt)
	return v.Leave(n)
}
