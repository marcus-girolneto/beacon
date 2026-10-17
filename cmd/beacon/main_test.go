package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

func TestRunVersion(t *testing.T) {
	t.Parallel()
	var out, errOut strings.Builder
	if code := run(t.Context(), []string{"-version"}, noEnv, &out, &errOut); code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.HasPrefix(out.String(), "beacon dev") {
		t.Errorf("output = %q", out.String())
	}
}

func TestRunHelpAndBadFlags(t *testing.T) {
	t.Parallel()
	var out, errOut strings.Builder
	if code := run(t.Context(), []string{"-h"}, noEnv, &out, &errOut); code != 0 {
		t.Errorf("-h exit code = %d", code)
	}
	if code := run(t.Context(), []string{"-sink", "kafka"}, noEnv, &out, &errOut); code != 2 {
		t.Errorf("bad flag exit code = %d, want 2", code)
	}
	if !strings.Contains(errOut.String(), "unknown sink") {
		t.Errorf("stderr = %q", errOut.String())
	}
}

func TestRunListenError(t *testing.T) {
	t.Parallel()
	var out, errOut strings.Builder
	if code := run(t.Context(), []string{"-addr", "256.0.0.1:99999"}, noEnv, &out, &errOut); code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunStartsAndStops(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	var out, errOut strings.Builder
	code := run(ctx, []string{"-addr", "127.0.0.1:0", "-log-format", "text"}, noEnv, &out, &errOut)
	if code != 0 {
		t.Fatalf("exit code = %d; stderr: %s", code, errOut.String())
	}
	if !strings.Contains(errOut.String(), "beacon started") || !strings.Contains(errOut.String(), "pipeline drained") {
		t.Errorf("unexpected logs: %s", errOut.String())
	}
}
