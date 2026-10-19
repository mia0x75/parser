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

package ast

import (
	"fmt"
	"strings"

	"github.com/mia0x75/parser/errors"
	"github.com/mia0x75/parser/format"
	"github.com/mia0x75/parser/types"
)

// LabeledStmt is implemented by compound statements that may carry a label.
type LabeledStmt interface {
	StmtNode
	// SetLabel sets the label of the statement.
	SetLabel(label string)
}

func restoreStmtList(ctx *format.RestoreCtx, list []StmtNode) error {
	for _, s := range list {
		if err := s.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore statement list")
		}
		ctx.WritePlain(";")
	}
	return nil
}

func acceptStmtList(v Visitor, list []StmtNode) ([]StmtNode, bool) {
	for i, s := range list {
		node, ok := s.Accept(v)
		if !ok {
			return list, false
		}
		list[i] = node.(StmtNode)
	}
	return list, true
}

// BeginEndStmt is a compound statement: `[label:] BEGIN ... END [label]`.
// See https://dev.mysql.com/doc/refman/8.0/en/begin-end.html
type BeginEndStmt struct {
	stmtNode

	Label string
	List  []StmtNode
}

// SetLabel implements LabeledStmt interface.
func (n *BeginEndStmt) SetLabel(label string) { n.Label = label }

// Restore implements Node interface.
func (n *BeginEndStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label != "" {
		ctx.WriteName(n.Label)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("BEGIN")
	if len(n.List) > 0 {
		ctx.WritePlain("\n")
		for _, s := range n.List {
			ctx.WritePlain("  ")
			if err := s.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore BeginEndStmt.List")
			}
			ctx.WritePlain(";")
			ctx.WritePlain("\n")
		}
	}
	ctx.WriteKeyWord("END")
	if n.Label != "" {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *BeginEndStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BeginEndStmt)
	list, ok := acceptStmtList(v, n.List)
	if !ok {
		return n, false
	}
	n.List = list
	return v.Leave(n)
}

// DeclareStmt is a variable declaration: `DECLARE var [, var]... type [DEFAULT expr]`.
// See https://dev.mysql.com/doc/refman/8.0/en/declare-local-variable.html
type DeclareStmt struct {
	stmtNode

	Names   []string
	Type    *types.FieldType
	Default ExprNode
}

// Restore implements Node interface.
func (n *DeclareStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	for i, name := range n.Names {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteName(name)
	}
	ctx.WritePlain(" ")
	if err := n.Type.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore DeclareStmt.Type")
	}
	if n.Default != nil {
		ctx.WriteKeyWord(" DEFAULT ")
		if err := n.Default.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DeclareStmt.Default")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DeclareStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareStmt)
	if n.Default != nil {
		node, ok := n.Default.Accept(v)
		if !ok {
			return n, false
		}
		n.Default = node.(ExprNode)
	}
	return v.Leave(n)
}

// HandlerConditionType is the type of a condition used by a handler or SIGNAL.
type HandlerConditionType int

// The types of handler conditions.
const (
	HandlerConditionSQLState HandlerConditionType = iota
	HandlerConditionErrorCode
	HandlerConditionName
	HandlerConditionSQLWarning
	HandlerConditionNotFound
	HandlerConditionSQLException
)

// HandlerCondition is a condition value used by a handler, SIGNAL or RESIGNAL.
type HandlerCondition struct {
	Type  HandlerConditionType
	Value string
}

// Restore implements Node interface.
func (c *HandlerCondition) Restore(ctx *format.RestoreCtx) error {
	switch c.Type {
	case HandlerConditionSQLState:
		ctx.WriteKeyWord("SQLSTATE VALUE ")
		ctx.WriteString(c.Value)
	case HandlerConditionErrorCode:
		ctx.WritePlain(c.Value)
	case HandlerConditionName:
		ctx.WriteName(c.Value)
	case HandlerConditionSQLWarning:
		ctx.WriteKeyWord("SQLWARNING")
	case HandlerConditionNotFound:
		ctx.WriteKeyWord("NOT FOUND")
	case HandlerConditionSQLException:
		ctx.WriteKeyWord("SQLEXCEPTION")
	}
	return nil
}

// NewErrnoHandlerCondition builds a handler condition from a MySQL error code.
func NewErrnoHandlerCondition(v interface{}) *HandlerCondition {
	return &HandlerCondition{Type: HandlerConditionErrorCode, Value: fmt.Sprintf("%v", v)}
}

// DeclareConditionStmt is a condition declaration:
// `DECLARE name CONDITION FOR condition_value`.
type DeclareConditionStmt struct {
	stmtNode

	Name      string
	Condition *HandlerCondition
}

// Restore implements Node interface.
func (n *DeclareConditionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" CONDITION FOR ")
	return n.Condition.Restore(ctx)
}

// Accept implements Node Accept interface.
func (n *DeclareConditionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareConditionStmt)
	return v.Leave(n)
}

// DeclareCursorStmt is a cursor declaration: `DECLARE name CURSOR FOR select`.
type DeclareCursorStmt struct {
	stmtNode

	Name   string
	Select StmtNode
}

// Restore implements Node interface.
func (n *DeclareCursorStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" CURSOR FOR ")
	return n.Select.Restore(ctx)
}

// Accept implements Node Accept interface.
func (n *DeclareCursorStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareCursorStmt)
	node, ok := n.Select.Accept(v)
	if !ok {
		return n, false
	}
	n.Select = node.(StmtNode)
	return v.Leave(n)
}

// HandlerType is the action of a handler declaration.
type HandlerType int

// The handler actions.
const (
	HandlerContinue HandlerType = iota
	HandlerExit
	HandlerUndo
)

// HandlerTypeString returns the SQL keyword of the handler action.
func HandlerTypeString(t HandlerType) string {
	switch t {
	case HandlerExit:
		return "EXIT"
	case HandlerUndo:
		return "UNDO"
	default:
		return "CONTINUE"
	}
}

// DeclareHandlerStmt is a handler declaration:
// `DECLARE {CONTINUE|EXIT|UNDO} HANDLER FOR condition [, condition]... stmt`.
type DeclareHandlerStmt struct {
	stmtNode

	Action     HandlerType
	Conditions []*HandlerCondition
	Stmt       StmtNode
}

// Restore implements Node interface.
func (n *DeclareHandlerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	ctx.WriteKeyWord(HandlerTypeString(n.Action))
	ctx.WriteKeyWord(" HANDLER FOR ")
	for i, c := range n.Conditions {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := c.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore DeclareHandlerStmt.Conditions")
		}
	}
	ctx.WritePlain(" ")
	return n.Stmt.Restore(ctx)
}

// Accept implements Node Accept interface.
func (n *DeclareHandlerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareHandlerStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(StmtNode)
	return v.Leave(n)
}

// IfClause is a single branch of an IfStmt.
type IfClause struct {
	Condition ExprNode
	List      []StmtNode
}

// IfStmt is an `IF ... THEN ... [ELSEIF ...] [ELSE ...] END IF` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/if.html
type IfStmt struct {
	stmtNode

	Conditions []*IfClause
	ElseList   []StmtNode
}

// Restore implements Node interface.
func (n *IfStmt) Restore(ctx *format.RestoreCtx) error {
	for i, c := range n.Conditions {
		if i == 0 {
			ctx.WriteKeyWord("IF ")
		} else {
			ctx.WriteKeyWord(" ELSEIF ")
		}
		if err := c.Condition.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore IfStmt.Conditions")
		}
		ctx.WriteKeyWord(" THEN ")
		if err := restoreStmtList(ctx, c.List); err != nil {
			return err
		}
	}
	if n.ElseList != nil {
		ctx.WriteKeyWord(" ELSE ")
		if err := restoreStmtList(ctx, n.ElseList); err != nil {
			return err
		}
	}
	ctx.WriteKeyWord(" END IF")
	return nil
}

// Accept implements Node Accept interface.
func (n *IfStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IfStmt)
	for _, c := range n.Conditions {
		node, ok := c.Condition.Accept(v)
		if !ok {
			return n, false
		}
		c.Condition = node.(ExprNode)
		list, ok := acceptStmtList(v, c.List)
		if !ok {
			return n, false
		}
		c.List = list
	}
	if n.ElseList != nil {
		list, ok := acceptStmtList(v, n.ElseList)
		if !ok {
			return n, false
		}
		n.ElseList = list
	}
	return v.Leave(n)
}

// CaseWhenStmt is a single WHEN branch of a CaseStmt.
type CaseWhenStmt struct {
	Expr ExprNode
	List []StmtNode
}

// CaseStmt is a `CASE ... WHEN ... THEN ... [ELSE ...] END CASE` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/case.html
type CaseStmt struct {
	stmtNode

	Value    ExprNode
	Whens    []*CaseWhenStmt
	ElseList []StmtNode
}

// Restore implements Node interface.
func (n *CaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CASE")
	if n.Value != nil {
		ctx.WritePlain(" ")
		if err := n.Value.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseStmt.Value")
		}
	}
	for _, w := range n.Whens {
		ctx.WriteKeyWord(" WHEN ")
		if err := w.Expr.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CaseStmt.Whens")
		}
		ctx.WriteKeyWord(" THEN ")
		if err := restoreStmtList(ctx, w.List); err != nil {
			return err
		}
	}
	if n.ElseList != nil {
		ctx.WriteKeyWord(" ELSE ")
		if err := restoreStmtList(ctx, n.ElseList); err != nil {
			return err
		}
	}
	ctx.WriteKeyWord(" END CASE")
	return nil
}

// Accept implements Node Accept interface.
func (n *CaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CaseStmt)
	if n.Value != nil {
		node, ok := n.Value.Accept(v)
		if !ok {
			return n, false
		}
		n.Value = node.(ExprNode)
	}
	for _, w := range n.Whens {
		node, ok := w.Expr.Accept(v)
		if !ok {
			return n, false
		}
		w.Expr = node.(ExprNode)
		list, ok := acceptStmtList(v, w.List)
		if !ok {
			return n, false
		}
		w.List = list
	}
	if n.ElseList != nil {
		list, ok := acceptStmtList(v, n.ElseList)
		if !ok {
			return n, false
		}
		n.ElseList = list
	}
	return v.Leave(n)
}

// WhileStmt is a `[label:] WHILE cond DO ... END WHILE [label]` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/while.html
type WhileStmt struct {
	stmtNode

	Label     string
	Condition ExprNode
	List      []StmtNode
}

// SetLabel implements LabeledStmt interface.
func (n *WhileStmt) SetLabel(label string) { n.Label = label }

// Restore implements Node interface.
func (n *WhileStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label != "" {
		ctx.WriteName(n.Label)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("WHILE ")
	if err := n.Condition.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore WhileStmt.Condition")
	}
	ctx.WriteKeyWord(" DO ")
	if err := restoreStmtList(ctx, n.List); err != nil {
		return err
	}
	ctx.WriteKeyWord(" END WHILE")
	if n.Label != "" {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *WhileStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*WhileStmt)
	node, ok := n.Condition.Accept(v)
	if !ok {
		return n, false
	}
	n.Condition = node.(ExprNode)
	list, ok := acceptStmtList(v, n.List)
	if !ok {
		return n, false
	}
	n.List = list
	return v.Leave(n)
}

// RepeatStmt is a `[label:] REPEAT ... UNTIL cond END REPEAT [label]` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/repeat.html
type RepeatStmt struct {
	stmtNode

	Label     string
	Condition ExprNode
	List      []StmtNode
}

// SetLabel implements LabeledStmt interface.
func (n *RepeatStmt) SetLabel(label string) { n.Label = label }

// Restore implements Node interface.
func (n *RepeatStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label != "" {
		ctx.WriteName(n.Label)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("REPEAT ")
	if err := restoreStmtList(ctx, n.List); err != nil {
		return err
	}
	ctx.WriteKeyWord(" UNTIL ")
	if err := n.Condition.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore RepeatStmt.Condition")
	}
	ctx.WriteKeyWord(" END REPEAT")
	if n.Label != "" {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RepeatStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RepeatStmt)
	list, ok := acceptStmtList(v, n.List)
	if !ok {
		return n, false
	}
	n.List = list
	node, ok := n.Condition.Accept(v)
	if !ok {
		return n, false
	}
	n.Condition = node.(ExprNode)
	return v.Leave(n)
}

// LoopStmt is a `[label:] LOOP ... END LOOP [label]` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/loop.html
type LoopStmt struct {
	stmtNode

	Label string
	List  []StmtNode
}

// SetLabel implements LabeledStmt interface.
func (n *LoopStmt) SetLabel(label string) { n.Label = label }

// Restore implements Node interface.
func (n *LoopStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label != "" {
		ctx.WriteName(n.Label)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("LOOP ")
	if err := restoreStmtList(ctx, n.List); err != nil {
		return err
	}
	ctx.WriteKeyWord(" END LOOP")
	if n.Label != "" {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *LoopStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LoopStmt)
	list, ok := acceptStmtList(v, n.List)
	if !ok {
		return n, false
	}
	n.List = list
	return v.Leave(n)
}

// LeaveStmt is a `LEAVE label` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/leave.html
type LeaveStmt struct {
	stmtNode

	Label string
}

// Restore implements Node interface.
func (n *LeaveStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LEAVE ")
	ctx.WriteName(n.Label)
	return nil
}

// Accept implements Node Accept interface.
func (n *LeaveStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// IterateStmt is an `ITERATE label` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/iterate.html
type IterateStmt struct {
	stmtNode

	Label string
}

// Restore implements Node interface.
func (n *IterateStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ITERATE ")
	ctx.WriteName(n.Label)
	return nil
}

// Accept implements Node Accept interface.
func (n *IterateStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// OpenStmt is an `OPEN cursor_name` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/open.html
type OpenStmt struct {
	stmtNode

	Name string
}

// Restore implements Node interface.
func (n *OpenStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("OPEN ")
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *OpenStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// CloseStmt is a `CLOSE cursor_name` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/close.html
type CloseStmt struct {
	stmtNode

	Name string
}

// Restore implements Node interface.
func (n *CloseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CLOSE ")
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *CloseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// FetchStmt is a `FETCH cursor_name INTO var [, var]...` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/fetch.html
type FetchStmt struct {
	stmtNode

	Name string
	Vars []string
}

// Restore implements Node interface.
func (n *FetchStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("FETCH ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" INTO ")
	for i, v := range n.Vars {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if strings.HasPrefix(v, "@") {
			ctx.WritePlain(v)
		} else {
			ctx.WriteName(v)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FetchStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// SignalInfoItem is a `name = expr` item of a SIGNAL or RESIGNAL statement.
type SignalInfoItem struct {
	Name  string
	Value ExprNode
}

// SignalStmt is a `SIGNAL condition [SET item = expr, ...]` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/signal.html
type SignalStmt struct {
	stmtNode

	Condition *HandlerCondition
	InfoItems []*SignalInfoItem
}

// Restore implements Node interface.
func (n *SignalStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SIGNAL ")
	if err := n.Condition.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore SignalStmt.Condition")
	}
	if len(n.InfoItems) > 0 {
		ctx.WriteKeyWord(" SET ")
		for i, item := range n.InfoItems {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(item.Name)
			ctx.WritePlain(" = ")
			if err := item.Value.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore SignalStmt.InfoItems")
			}
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SignalStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SignalStmt)
	for _, item := range n.InfoItems {
		node, ok := item.Value.Accept(v)
		if !ok {
			return n, false
		}
		item.Value = node.(ExprNode)
	}
	return v.Leave(n)
}

// ResignalStmt is a `RESIGNAL [condition] [SET item = expr, ...]` statement.
// See https://dev.mysql.com/doc/refman/8.0/en/resignal.html
type ResignalStmt struct {
	stmtNode

	Condition *HandlerCondition
	InfoItems []*SignalInfoItem
}

// Restore implements Node interface.
func (n *ResignalStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESIGNAL")
	if n.Condition != nil {
		ctx.WritePlain(" ")
		if err := n.Condition.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore ResignalStmt.Condition")
		}
	}
	if len(n.InfoItems) > 0 {
		ctx.WriteKeyWord(" SET ")
		for i, item := range n.InfoItems {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(item.Name)
			ctx.WritePlain(" = ")
			if err := item.Value.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore ResignalStmt.InfoItems")
			}
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ResignalStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ResignalStmt)
	for _, item := range n.InfoItems {
		node, ok := item.Value.Accept(v)
		if !ok {
			return n, false
		}
		item.Value = node.(ExprNode)
	}
	return v.Leave(n)
}
