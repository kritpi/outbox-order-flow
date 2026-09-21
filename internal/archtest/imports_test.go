// Package archtest enforces the import rules from ADR-0005. Go's internal/ directory only
// stops imports from outside the module, so without this test nothing would prevent
// internal/inventory from importing internal/order.
//
// Checking direct imports is enough: any indirect path from one service to another
// contains a direct import that breaks a rule.
package archtest

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// services maps each cmd/ directory to the one service package it may wire.
var services = map[string]string{
	"order-service":        "order",
	"inventory-service":    "inventory",
	"notification-service": "notification",
}

// allowedPrefixes returns the module directories that Go code in dir may import. A dir
// with no rule is an error, so a new top-level package (a shared internal/events, for
// example) fails the test until someone decides where it sits.
func allowedPrefixes(dir string) ([]string, error) {
	parts := strings.Split(dir, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("%s: Go code outside cmd/<name> and internal/<name> has no import rule", dir)
	}
	switch parts[0] {
	case "cmd":
		svc, ok := services[parts[1]]
		if !ok {
			return nil, fmt.Errorf("%s: unknown command; map it to its service package in archtest's services", dir)
		}
		return []string{"internal/" + svc, "internal/platform"}, nil
	case "internal":
		switch parts[1] {
		case "platform":
			return []string{"internal/platform"}, nil
		case "archtest":
			return nil, nil
		}
		for _, svc := range services {
			if parts[1] == svc {
				return []string{"internal/" + svc, "internal/platform"}, nil
			}
		}
		return nil, fmt.Errorf("%s: unclassified package; give it an import rule in archtest first (event contracts are consumer-defined, ADR-0007)", dir)
	}
	return nil, fmt.Errorf("%s: Go code outside cmd/ and internal/ has no import rule", dir)
}

// checkImport reports whether code in dir (slash-separated, relative to the module root)
// may import imp. Imports outside the module (stdlib, third party) are always allowed.
func checkImport(module, dir, imp string) error {
	if imp != module && !strings.HasPrefix(imp, module+"/") {
		return nil
	}
	target := strings.TrimPrefix(strings.TrimPrefix(imp, module), "/")

	allowed, err := allowedPrefixes(dir)
	if err != nil {
		return err
	}
	for _, p := range allowed {
		if target == p || strings.HasPrefix(target, p+"/") {
			return nil
		}
	}
	if len(allowed) == 0 {
		return fmt.Errorf("%s must not import %s (allowed: nothing in this module)", dir, target)
	}
	return fmt.Errorf("%s must not import %s (allowed: %s)", dir, target, strings.Join(allowed, ", "))
}

func TestCheckImport(t *testing.T) {
	const mod = "example.com/m"
	tests := []struct {
		dir, imp string
		ok       bool
	}{
		{"cmd/order-service", mod + "/internal/order", true},
		{"cmd/order-service", mod + "/internal/platform/pg", true},
		{"cmd/order-service", mod + "/internal/inventory", false},
		{"internal/order", mod + "/internal/order/store", true},
		{"internal/order", mod + "/internal/platform/shutdown", true},
		{"internal/order", "net/http", true},
		{"internal/order", "github.com/jackc/pgx/v5", true},
		{"internal/order", "example.com/mx/internal/inventory", true}, // another module sharing a prefix
		{"internal/inventory", mod + "/internal/order", false},
		{"internal/inventory/store", mod + "/internal/notification", false},
		{"internal/platform/outbox", mod + "/internal/platform/pg", true},
		{"internal/platform/outbox", mod + "/internal/order", false},
		{"internal/archtest", mod + "/internal/platform/pg", false},
		{"internal/events", mod + "/internal/platform/pg", false}, // unclassified package
		{"cmd/mystery-service", mod + "/internal/order", false},   // unmapped command
		{"pkg/util", mod + "/internal/order", false},              // outside cmd/ and internal/
	}
	for _, tc := range tests {
		err := checkImport(mod, tc.dir, tc.imp)
		if (err == nil) != tc.ok {
			t.Errorf("checkImport(%q, %q) = %v, want allowed=%v", tc.dir, tc.imp, err, tc.ok)
		}
	}
}

func TestRepositoryImports(t *testing.T) {
	root, module := moduleRoot(t)
	fset := token.NewFileSet()
	dirs := map[string]bool{}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		dir := filepath.ToSlash(rel)
		if !dirs[dir] {
			dirs[dir] = true
			if _, err := allowedPrefixes(dir); err != nil {
				t.Error(err)
			}
		}

		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range f.Imports {
			imp, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if err := checkImport(module, dir, imp); err != nil {
				t.Errorf("%s: %v", fset.Position(spec.Pos()), err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) == 0 {
		t.Fatalf("no Go files found under %s", root)
	}
}

// moduleRoot walks up from the test's working directory to go.mod and returns that
// directory and the module path. Parsing the module line by hand avoids depending on
// golang.org/x/mod for one line.
func moduleRoot(t *testing.T) (dir, module string) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
					return dir, strings.Trim(strings.TrimSpace(rest), `"`)
				}
			}
			t.Fatalf("%s/go.mod has no module line", dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the working directory")
		}
		dir = parent
	}
}
