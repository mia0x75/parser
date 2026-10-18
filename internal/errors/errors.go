// Package errors provides error handling primitives with stack traces.
//
// It is a minimal, stdlib-only reimplementation of the subset of
// github.com/pingcap/errors used by this module. Only the functions actually
// referenced by the code base are provided: New, Errorf, Annotate, Annotatef,
// Trace, AddStack, Cause and ErrorStack.
//
// Errors produced by this package implement fmt.Formatter and support the
// %s, %v and %+v verbs; %+v additionally prints the recorded stack trace.
package errors

import (
	"fmt"
	"io"
)

// New returns an error with the supplied message.
// New also records the stack trace at the point it was called.
func New(message string) error {
	return &fundamental{
		msg:   message,
		stack: callers(),
	}
}

// Errorf formats according to a format specifier and returns the string as a
// value that satisfies error.
// Errorf also records the stack trace at the point it was called.
func Errorf(format string, args ...any) error {
	return &fundamental{
		msg:   fmt.Sprintf(format, args...),
		stack: callers(),
	}
}

// fundamental is an error that has a message and a stack, but no caller.
type fundamental struct {
	msg string
	*stack
}

func (f *fundamental) Error() string { return f.msg }

func (f *fundamental) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, f.msg)
			f.stack.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, f.msg)
	case 'q':
		fmt.Fprintf(s, "%q", f.msg)
	}
}

// Trace annotates err with a stack trace at the point Trace was called.
// If err is nil, Trace returns nil. If err already carries a stack trace it
// is returned unchanged.
func Trace(err error) error {
	return AddStack(err)
}

// AddStack annotates err with a stack trace at the point AddStack was called,
// unless the error chain already carries one.
// If err is nil, AddStack returns nil.
func AddStack(err error) error {
	if err == nil || hasStack(err) {
		return err
	}
	return &withStack{
		error: err,
		stack: callers(),
	}
}

type withStack struct {
	error
	*stack
}

// Cause returns the next error in the chain.
func (w *withStack) Cause() error { return w.error }

// Unwrap provides compatibility for Go 1.13 error chains.
func (w *withStack) Unwrap() error { return w.error }

func (w *withStack) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.Cause())
			w.stack.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, w.Error())
	case 'q':
		fmt.Fprintf(s, "%q", w.Error())
	}
}

// Annotate adds a message to err and ensures there is a stack trace.
// If err is nil, Annotate returns nil.
func Annotate(err error, message string) error {
	if err == nil {
		return nil
	}
	has := hasStack(err)
	err = &withMessage{
		cause:         err,
		msg:           message,
		causeHasStack: has,
	}
	if has {
		return err
	}
	return &withStack{
		error: err,
		stack: callers(),
	}
}

// Annotatef adds a formatted message to err and ensures there is a stack trace.
// If err is nil, Annotatef returns nil.
func Annotatef(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	has := hasStack(err)
	err = &withMessage{
		cause:         err,
		msg:           fmt.Sprintf(format, args...),
		causeHasStack: has,
	}
	if has {
		return err
	}
	return &withStack{
		error: err,
		stack: callers(),
	}
}

type withMessage struct {
	cause         error
	msg           string
	causeHasStack bool
}

func (w *withMessage) Error() string  { return w.msg + ": " + w.cause.Error() }
func (w *withMessage) Cause() error   { return w.cause }
func (w *withMessage) Unwrap() error  { return w.cause }
func (w *withMessage) HasStack() bool { return w.causeHasStack }

func (w *withMessage) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n", w.Cause())
			io.WriteString(s, w.msg)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, w.Error())
	case 'q':
		fmt.Fprintf(s, "%q", w.Error())
	}
}

// Cause returns the underlying cause of the error, if possible.
// An error value has a cause if it implements the following interface:
//
//	type causer interface {
//		Cause() error
//	}
//
// If the error does not implement Cause, the original error will be returned.
// If the error is nil, nil will be returned without further investigation.
func Cause(err error) error {
	cause := unwrap(err)
	if cause == nil {
		return err
	}
	return Cause(cause)
}

// unwrap uses causer to return the next error in the chain or nil.
func unwrap(err error) error {
	type causer interface {
		Cause() error
	}
	if unErr, ok := err.(causer); ok {
		return unErr.Cause()
	}
	return nil
}

type stackTracer interface {
	Empty() bool
}

type stackAware interface {
	HasStack() bool
}

// hasStack tells whether any error in the chain carries a non-empty stack.
func hasStack(err error) bool {
	for err != nil {
		if aware, ok := err.(stackAware); ok {
			return aware.HasStack()
		}
		if tracer, ok := err.(stackTracer); ok {
			return !tracer.Empty()
		}
		type causer interface {
			Cause() error
		}
		c, ok := err.(causer)
		if !ok {
			return false
		}
		err = c.Cause()
	}
	return false
}

// ErrorStack will format a stack trace if it is available, otherwise it will
// return Error(). If the error is nil, the empty string is returned.
// Note that this just calls fmt.Sprintf("%+v", err).
func ErrorStack(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%+v", err)
}
