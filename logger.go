package main

import (
	"log/slog"
	"os"
)

// setupLogging installs a process-wide JSON structured logger as the slog
// default. JSON lines are trivially ingested by Loki/Datadog/jq, which is why
// we prefer them over free-form text for a production service.
func setupLogging() {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(h))
}
