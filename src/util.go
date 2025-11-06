package raft

import (
	"fmt"
	"log/slog"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		slog.Debug(fmt.Sprintf(format, a...))
	}
	return
}
