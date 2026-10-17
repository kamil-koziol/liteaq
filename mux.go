package liteaq

import (
	"context"
)

type ServeMux struct {
	handlers map[string]HandlerFunc
}

func NewServeMux() *ServeMux {
	return &ServeMux{
		handlers: map[string]HandlerFunc{},
	}
}

type HandlerFunc func(ctx context.Context, task *JobDetails) error

func (m *ServeMux) registerHandleFunc(typ string, fn HandlerFunc) {
	if _, exists := m.getHandler(typ); exists {
		panic("multiple registrations for " + typ)
	}

	m.handlers[typ] = fn
}

func (m *ServeMux) getHandler(typ string) (HandlerFunc, bool) {
	fn, exists := m.handlers[typ]
	return fn, exists
}

func (m *ServeMux) HandleFunc(typ string, fn HandlerFunc) {
	if fn == nil {
		panic("nil handler passed for " + typ)
	}

	m.registerHandleFunc(typ, fn)
}

type Handler interface {
	ProcessTask(ctx context.Context, task *JobDetails) error
}

func (m *ServeMux) Handle(typ string, handler Handler) {
	if handler == nil {
		panic("liteaq: nil handler")
	}

	m.registerHandleFunc(typ, handler.ProcessTask)
}
