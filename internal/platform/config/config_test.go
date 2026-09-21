package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/kritpi/outbox-order-flow/internal/platform/config"
)

func lookupFrom(vars map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	}
}

func TestEnvReadsValuesAndDefaults(t *testing.T) {
	env := config.New(lookupFrom(map[string]string{
		"DATABASE_URL":     "postgres://x",
		"SHUTDOWN_TIMEOUT": "3s",
		"DB_MAX_CONNS":     "8",
		"EMPTY":            "",
	}))

	if got := env.Required("DATABASE_URL"); got != "postgres://x" {
		t.Errorf("Required = %q, want postgres://x", got)
	}
	if got := env.Duration("SHUTDOWN_TIMEOUT", time.Second); got != 3*time.Second {
		t.Errorf("Duration = %v, want 3s", got)
	}
	if got := env.Int("DB_MAX_CONNS", 4); got != 8 {
		t.Errorf("Int = %d, want 8", got)
	}
	if got := env.String("EMPTY", "fallback"); got != "fallback" {
		t.Errorf("String on empty var = %q, want the default", got)
	}
	if got := env.Duration("UNSET", 5*time.Second); got != 5*time.Second {
		t.Errorf("Duration on unset var = %v, want the default", got)
	}
	if err := env.Err(); err != nil {
		t.Errorf("Err = %v, want nil", err)
	}
}

func TestEnvReportsEveryProblemAtOnce(t *testing.T) {
	env := config.New(lookupFrom(map[string]string{
		"SHUTDOWN_TIMEOUT": "ten seconds",
		"DB_MAX_CONNS":     "many",
	}))

	env.Required("DATABASE_URL")
	env.Duration("SHUTDOWN_TIMEOUT", time.Second)
	env.Int("DB_MAX_CONNS", 4)

	err := env.Err()
	if err == nil {
		t.Fatal("Err = nil, want an error naming every bad variable")
	}
	for _, want := range []string{"DATABASE_URL is required", "SHUTDOWN_TIMEOUT", "DB_MAX_CONNS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Err = %q, missing %q", err, want)
		}
	}
}
