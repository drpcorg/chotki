package utils

import (
	"log/slog"
	"os"
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
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
