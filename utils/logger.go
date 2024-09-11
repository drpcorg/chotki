package utils

import (
	"context"
	"log/slog"
	"os"
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	DebugCtx(ctx context.Context, msg string, args ...any)
	InfoCtx(ctx context.Context, msg string, args ...any)
	WarnCtx(ctx context.Context, msg string, args ...any)
	ErrorCtx(ctx context.Context, msg string, args ...any)
}

type DefaultLogger struct {
	logger *slog.Logger
}

func NewDefaultLogger(level slog.Level) *DefaultLogger {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))

	return &DefaultLogger{logger: logger}
}

const prefix = "[chotki] "

func (d *DefaultLogger) Debug(msg string, args ...any) {
	d.logger.Debug(prefix+msg, args...)
}

func (d *DefaultLogger) Info(msg string, args ...any) {
	d.logger.Info(prefix+msg, args...)
}

func (d *DefaultLogger) Warn(msg string, args ...any) {
	d.logger.Warn(prefix+msg, args...)
}

func (d *DefaultLogger) Error(msg string, args ...any) {
	d.logger.Error(prefix+msg, args...)
}

var DefaultArgs int

func getDefaultArgs(ctx context.Context) []any {
	ctxargs := ctx.Value(&DefaultArgs)
	if ctxargs == nil {
		ctxargs = make([]any, 0)
	}
	return ctxargs.([]any)
}

func WithDefaultArgs(ctx context.Context, args ...any) context.Context {
	dargs := getDefaultArgs(ctx)
	dargs = append(dargs, args...)
	return context.WithValue(ctx, &DefaultArgs, dargs)
}

func (d *DefaultLogger) DebugCtx(ctx context.Context, msg string, args ...any) {
	args = append(args, getDefaultArgs(ctx)...)
	d.logger.Debug(prefix+msg, args...)
}

func (d *DefaultLogger) InfoCtx(ctx context.Context, msg string, args ...any) {
	args = append(args, getDefaultArgs(ctx)...)
	d.logger.Info(prefix+msg, args...)
}

func (d *DefaultLogger) WarnCtx(ctx context.Context, msg string, args ...any) {
	args = append(args, getDefaultArgs(ctx)...)
	d.logger.Warn(prefix+msg, args...)
}

func (d *DefaultLogger) ErrorCtx(ctx context.Context, msg string, args ...any) {
	args = append(args, getDefaultArgs(ctx)...)
	d.logger.Error(prefix+msg, args...)
}
