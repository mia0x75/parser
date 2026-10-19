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
	"github.com/mia0x75/parser/auth"
	"github.com/mia0x75/parser/errors"
	"github.com/mia0x75/parser/format"
	"github.com/mia0x75/parser/model"
	"github.com/mia0x75/parser/types"
)

// FuncParamDir is the direction of a stored routine parameter.
type FuncParamDir int

// Directions of a stored routine parameter.
const (
	FuncParamDirIn FuncParamDir = iota
	FuncParamDirOut
	FuncParamDirInOut
)

// String returns the SQL keyword of the parameter direction.
func (d FuncParamDir) String() string {
	switch d {
	case FuncParamDirOut:
		return "OUT"
	case FuncParamDirInOut:
		return "INOUT"
	default:
		return "IN"
	}
}

// FuncParam is a parameter of a stored function or procedure.
type FuncParam struct {
	Name string
	Type *types.FieldType
	Dir  FuncParamDir
}

// Restore implements Node interface.
func (p *FuncParam) Restore(ctx *format.RestoreCtx) error {
	if p.Dir != FuncParamDirIn {
		ctx.WriteKeyWord(p.Dir.String())
		ctx.WritePlain(" ")
	}
	ctx.WriteName(p.Name)
	ctx.WritePlain(" ")
	return p.Type.Restore(ctx)
}

// RoutineDataAccess describes the data-access characteristic of a routine.
type RoutineDataAccess int

// Data-access characteristics.
const (
	RoutineDataAccessNoSQL RoutineDataAccess = iota
	RoutineDataAccessContainsSQL
	RoutineDataAccessReadsSQLData
	RoutineDataAccessModifiesSQLData
)

// RoutineCharacteristics holds the optional characteristics of a stored
// function or procedure.
type RoutineCharacteristics struct {
	Language               string
	LanguageSpecified      bool
	Deterministic          bool
	DeterministicSpecified bool
	DataAccess             RoutineDataAccess
	DataAccessSpecified    bool
	Security               model.ViewSecurity
	SecuritySpecified      bool
	Comment                string
	CommentSpecified       bool
}

// HasAny reports whether at least one characteristic was specified.
func (c *RoutineCharacteristics) HasAny() bool {
	if c == nil {
		return false
	}
	return c.LanguageSpecified || c.DeterministicSpecified || c.DataAccessSpecified ||
		c.SecuritySpecified || c.CommentSpecified
}

// Restore implements Node interface. Each clause is prefixed with a leading
// space so that an empty characteristic list renders nothing.
func (c *RoutineCharacteristics) Restore(ctx *format.RestoreCtx) error {
	if c == nil {
		return nil
	}
	if c.LanguageSpecified {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord("LANGUAGE ")
		ctx.WriteKeyWord(c.Language)
	}
	if c.DeterministicSpecified {
		ctx.WritePlain(" ")
		if !c.Deterministic {
			ctx.WriteKeyWord("NOT ")
		}
		ctx.WriteKeyWord("DETERMINISTIC")
	}
	if c.DataAccessSpecified {
		ctx.WritePlain(" ")
		switch c.DataAccess {
		case RoutineDataAccessContainsSQL:
			ctx.WriteKeyWord("CONTAINS SQL")
		case RoutineDataAccessReadsSQLData:
			ctx.WriteKeyWord("READS SQL DATA")
		case RoutineDataAccessModifiesSQLData:
			ctx.WriteKeyWord("MODIFIES SQL DATA")
		default:
			ctx.WriteKeyWord("NO SQL")
		}
	}
	if c.SecuritySpecified {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord("SQL SECURITY ")
		ctx.WriteKeyWord(c.Security.String())
	}
	if c.CommentSpecified {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord("COMMENT ")
		ctx.WriteString(c.Comment)
	}
	return nil
}

// EventSchedule is the ON SCHEDULE clause of a CREATE/ALTER EVENT statement.
type EventSchedule struct {
	At       ExprNode
	Every    ExprNode
	Interval string
	Starts   ExprNode
	Ends     ExprNode
}

// Restore implements Node interface.
func (s *EventSchedule) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ON SCHEDULE ")
	if s.At != nil {
		ctx.WriteKeyWord("AT ")
		if err := s.At.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore EventSchedule.At")
		}
		return nil
	}
	ctx.WriteKeyWord("EVERY ")
	if err := s.Every.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore EventSchedule.Every")
	}
	if s.Interval != "" {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(s.Interval)
	}
	if s.Starts != nil {
		ctx.WriteKeyWord(" STARTS ")
		if err := s.Starts.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore EventSchedule.Starts")
		}
	}
	if s.Ends != nil {
		ctx.WriteKeyWord(" ENDS ")
		if err := s.Ends.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore EventSchedule.Ends")
		}
	}
	return nil
}

// EventStatus is the [ENABLE | DISABLE | DISABLE ON SLAVE] clause of an event.
type EventStatus int

// Event statuses.
const (
	EventStatusDefault EventStatus = iota
	EventStatusEnable
	EventStatusDisable
	EventStatusDisableOnSlave
)

// CreateFunctionStmt is a statement to create a stored function.
// See https://dev.mysql.com/doc/refman/8.0/en/create-procedure.html
type CreateFunctionStmt struct {
	ddlNode

	OrReplace       bool
	IfNotExists     bool
	Definer         *auth.UserIdentity
	Name            *TableName
	Params          []*FuncParam
	Returns         *types.FieldType
	Characteristics *RoutineCharacteristics
	Body            StmtNode
}

// Restore implements Node interface.
func (n *CreateFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("DEFINER")
	ctx.WritePlain(" = ")
	if err := n.Definer.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateFunctionStmt.Definer")
	}
	ctx.WriteKeyWord(" FUNCTION ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateFunctionStmt.Name")
	}
	if err := restoreFuncParams(ctx, n.Params); err != nil {
		return err
	}
	ctx.WriteKeyWord(" RETURNS ")
	if err := n.Returns.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateFunctionStmt.Returns")
	}
	if err := n.Characteristics.Restore(ctx); err != nil {
		return err
	}
	ctx.WritePlain(" ")
	if err := n.Body.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateFunctionStmt.Body")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateFunctionStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	body, ok := n.Body.Accept(v)
	if !ok {
		return n, false
	}
	n.Body = body.(StmtNode)
	return v.Leave(n)
}

// CreateProcedureStmt is a statement to create a stored procedure.
// See https://dev.mysql.com/doc/refman/8.0/en/create-procedure.html
type CreateProcedureStmt struct {
	ddlNode

	OrReplace       bool
	IfNotExists     bool
	Definer         *auth.UserIdentity
	Name            *TableName
	Params          []*FuncParam
	Characteristics *RoutineCharacteristics
	Body            StmtNode
}

// Restore implements Node interface.
func (n *CreateProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("DEFINER")
	ctx.WritePlain(" = ")
	if err := n.Definer.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateProcedureStmt.Definer")
	}
	ctx.WriteKeyWord(" PROCEDURE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateProcedureStmt.Name")
	}
	if err := restoreFuncParams(ctx, n.Params); err != nil {
		return err
	}
	if err := n.Characteristics.Restore(ctx); err != nil {
		return err
	}
	ctx.WritePlain(" ")
	if err := n.Body.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateProcedureStmt.Body")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateProcedureStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	body, ok := n.Body.Accept(v)
	if !ok {
		return n, false
	}
	n.Body = body.(StmtNode)
	return v.Leave(n)
}

// AlterFunctionStmt is a statement to change the characteristics of a stored
// function.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-function.html
type AlterFunctionStmt struct {
	ddlNode

	Name            *TableName
	Characteristics *RoutineCharacteristics
}

// Restore implements Node interface.
func (n *AlterFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER FUNCTION ")
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterFunctionStmt.Name")
	}
	return n.Characteristics.Restore(ctx)
}

// Accept implements Node Accept interface.
func (n *AlterFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterFunctionStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	return v.Leave(n)
}

// AlterProcedureStmt is a statement to change the characteristics of a stored
// procedure.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-procedure.html
type AlterProcedureStmt struct {
	ddlNode

	Name            *TableName
	Characteristics *RoutineCharacteristics
}

// Restore implements Node interface.
func (n *AlterProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER PROCEDURE ")
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterProcedureStmt.Name")
	}
	return n.Characteristics.Restore(ctx)
}

// Accept implements Node Accept interface.
func (n *AlterProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterProcedureStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	return v.Leave(n)
}

// CreateTriggerStmt is a statement to create a trigger.
// See https://dev.mysql.com/doc/refman/8.0/en/create-trigger.html
type CreateTriggerStmt struct {
	ddlNode

	OrReplace        bool
	IfNotExists      bool
	Definer          *auth.UserIdentity
	Name             *TableName
	Time             string
	Event            string
	UpdateColumns    []*ColumnName
	Table            *TableName
	Order            string
	OtherTriggerName string
	Body             StmtNode
}

// Restore implements Node interface.
func (n *CreateTriggerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.OrReplace {
		ctx.WriteKeyWord("OR REPLACE ")
	}
	ctx.WriteKeyWord("DEFINER")
	ctx.WritePlain(" = ")
	if err := n.Definer.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateTriggerStmt.Definer")
	}
	ctx.WriteKeyWord(" TRIGGER ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateTriggerStmt.Name")
	}
	ctx.WriteKeyWord(" " + n.Time + " " + n.Event)
	if n.Event == "UPDATE" && len(n.UpdateColumns) > 0 {
		ctx.WriteKeyWord(" OF ")
		for i, col := range n.UpdateColumns {
			if i > 0 {
				ctx.WritePlain(", ")
			}
			if err := col.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore CreateTriggerStmt.UpdateColumns")
			}
		}
	}
	ctx.WriteKeyWord(" ON ")
	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateTriggerStmt.Table")
	}
	ctx.WriteKeyWord(" FOR EACH ROW ")
	if n.Order != "" {
		ctx.WriteKeyWord(n.Order)
		ctx.WritePlain(" ")
		ctx.WriteName(n.OtherTriggerName)
		ctx.WritePlain(" ")
	}
	if err := n.Body.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateTriggerStmt.Body")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateTriggerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateTriggerStmt)
	table, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = table.(*TableName)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	body, ok := n.Body.Accept(v)
	if !ok {
		return n, false
	}
	n.Body = body.(StmtNode)
	return v.Leave(n)
}

// CreateEventStmt is a statement to create an event.
// See https://dev.mysql.com/doc/refman/8.0/en/create-event.html
type CreateEventStmt struct {
	ddlNode

	Definer          *auth.UserIdentity
	IfNotExists      bool
	Name             *TableName
	Schedule         *EventSchedule
	OnCompletion     bool
	Preserve         bool
	Status           EventStatus
	Comment          string
	CommentSpecified bool
	Body             StmtNode
}

// Restore implements Node interface.
func (n *CreateEventStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE DEFINER")
	ctx.WritePlain(" = ")
	if err := n.Definer.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateEventStmt.Definer")
	}
	ctx.WriteKeyWord(" EVENT ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore CreateEventStmt.Name")
	}
	ctx.WritePlain(" ")
	if err := n.Schedule.Restore(ctx); err != nil {
		return err
	}
	if n.OnCompletion {
		ctx.WriteKeyWord(" ON COMPLETION ")
		if !n.Preserve {
			ctx.WriteKeyWord("NOT ")
		}
		ctx.WriteKeyWord("PRESERVE")
	}
	restoreEventStatus(ctx, n.Status)
	if n.CommentSpecified {
		ctx.WriteKeyWord(" COMMENT ")
		ctx.WriteString(n.Comment)
	}
	ctx.WriteKeyWord(" DO ")
	return n.Body.Restore(ctx)
}

// Accept implements Node Accept interface.
func (n *CreateEventStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateEventStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	body, ok := n.Body.Accept(v)
	if !ok {
		return n, false
	}
	n.Body = body.(StmtNode)
	return v.Leave(n)
}

// AlterEventStmt is a statement to alter an existing event.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-event.html
type AlterEventStmt struct {
	ddlNode

	Definer          *auth.UserIdentity
	Name             *TableName
	Schedule         *EventSchedule
	OnCompletion     bool
	Preserve         bool
	RenameName       *TableName
	Status           EventStatus
	Comment          string
	CommentSpecified bool
	Body             StmtNode
}

// Restore implements Node interface.
func (n *AlterEventStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER DEFINER")
	ctx.WritePlain(" = ")
	if err := n.Definer.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterEventStmt.Definer")
	}
	ctx.WriteKeyWord(" EVENT ")
	if err := n.Name.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore AlterEventStmt.Name")
	}
	if n.Schedule != nil {
		ctx.WritePlain(" ")
		if err := n.Schedule.Restore(ctx); err != nil {
			return err
		}
	}
	if n.OnCompletion {
		ctx.WriteKeyWord(" ON COMPLETION ")
		if !n.Preserve {
			ctx.WriteKeyWord("NOT ")
		}
		ctx.WriteKeyWord("PRESERVE")
	}
	if n.RenameName != nil {
		ctx.WriteKeyWord(" RENAME TO ")
		if err := n.RenameName.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterEventStmt.RenameName")
		}
	}
	restoreEventStatus(ctx, n.Status)
	if n.CommentSpecified {
		ctx.WriteKeyWord(" COMMENT ")
		ctx.WriteString(n.Comment)
	}
	if n.Body != nil {
		ctx.WriteKeyWord(" DO ")
		if err := n.Body.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore AlterEventStmt.Body")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterEventStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterEventStmt)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*TableName)
	if n.RenameName != nil {
		renameNode, ok := n.RenameName.Accept(v)
		if !ok {
			return n, false
		}
		n.RenameName = renameNode.(*TableName)
	}
	if n.Body != nil {
		body, ok := n.Body.Accept(v)
		if !ok {
			return n, false
		}
		n.Body = body.(StmtNode)
	}
	return v.Leave(n)
}

// ReturnStmt is the RETURN statement used in stored functions.
// See https://dev.mysql.com/doc/refman/8.0/en/return.html
type ReturnStmt struct {
	stmtNode

	Expr ExprNode
}

// Restore implements Node interface.
func (n *ReturnStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RETURN ")
	if err := n.Expr.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ReturnStmt.Expr")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ReturnStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ReturnStmt)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

func restoreFuncParams(ctx *format.RestoreCtx, params []*FuncParam) error {
	ctx.WritePlain("(")
	for i, p := range params {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := p.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore routine parameters")
		}
	}
	ctx.WritePlain(")")
	return nil
}

func restoreEventStatus(ctx *format.RestoreCtx, status EventStatus) {
	switch status {
	case EventStatusEnable:
		ctx.WriteKeyWord(" ENABLE")
	case EventStatusDisable:
		ctx.WriteKeyWord(" DISABLE")
	case EventStatusDisableOnSlave:
		ctx.WriteKeyWord(" DISABLE ON SLAVE")
	}
}
