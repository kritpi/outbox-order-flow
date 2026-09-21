// Package config reads service configuration from environment variables.
//
// Env records every problem instead of stopping at the first, so a misconfigured
// service reports all of its missing or invalid variables in one startup error.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Env reads variables through a lookup function, which tests replace with a map.
type Env struct {
	lookup func(string) (string, bool)
	errs   []error
}

// FromOS returns an Env backed by the process environment.
func FromOS() *Env { return New(os.LookupEnv) }

// New returns an Env backed by lookup.
func New(lookup func(string) (string, bool)) *Env { return &Env{lookup: lookup} }

// String returns the variable's value, or def when it is unset or empty.
func (e *Env) String(key, def string) string {
	if v, ok := e.lookup(key); ok && v != "" {
		return v
	}
	return def
}

// Required returns the variable's value and records an error when it is unset or empty.
func (e *Env) Required(key string) string {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		e.errs = append(e.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

// Int parses the variable as a base-10 integer, or returns def when it is unset or empty.
func (e *Env) Int(key string, def int) int {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

// Duration parses the variable with time.ParseDuration (e.g. "10s"), or returns def
// when it is unset or empty.
func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v, ok := e.lookup(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}

// Err joins every error recorded so far, or returns nil.
func (e *Env) Err() error { return errors.Join(e.errs...) }
