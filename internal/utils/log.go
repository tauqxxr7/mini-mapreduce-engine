package utils

import (
	"log/slog"
	"os"
)

func NewLogger(component string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(handler).With("component", component)
}
