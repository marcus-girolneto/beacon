package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	var out, errOut strings.Builder
	code := run(t.Context(), []string{"-target", srv.URL, "-duration", "100ms", "-concurrency", "2", "-batch", "5"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("exit code = %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "status 202") {
		t.Errorf("output = %s", out.String())
	}
}

func TestRunFlags(t *testing.T) {
	t.Parallel()
	var out, errOut strings.Builder
	if code := run(t.Context(), []string{"-version"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "loadgen dev") {
		t.Errorf("-version: code=%d out=%q", code, out.String())
	}
	if code := run(t.Context(), []string{"-h"}, &out, &errOut); code != 0 {
		t.Errorf("-h exit code = %d", code)
	}
	if code := run(t.Context(), []string{"-nope"}, &out, &errOut); code != 2 {
		t.Errorf("bad flag exit code = %d", code)
	}
	if code := run(t.Context(), []string{"-concurrency", "0"}, &out, &errOut); code != 1 {
		t.Errorf("invalid config exit code = %d", code)
	}
}
