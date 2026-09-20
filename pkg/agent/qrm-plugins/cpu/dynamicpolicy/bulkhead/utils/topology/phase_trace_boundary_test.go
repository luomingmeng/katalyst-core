/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topology

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParentSafeValidatedTraceProductionBoundary(t *testing.T) {
	files := parseTopologyProductionFiles(t)

	var freezeCallers []string
	functions := make(map[string]*ast.FuncDecl)
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			functions[function.Name.Name] = function
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				identifier, ok := call.Fun.(*ast.Ident)
				if ok && identifier.Name == "freezeValidatedPhaseTrace" {
					freezeCallers = append(freezeCallers, function.Name.Name)
				}
				return true
			})
		}
	}

	require.Equal(t, []string{"compileValidatedFixedPointTrace"}, freezeCallers,
		"only the compiler boundary may construct a validated carrier")
	for _, name := range []string{
		"ReservePhaseTrace",
		"preflightFrozenTrace",
		"preflightFrozenTraceOperations",
		"executeFrozenTrace",
	} {
		require.NotContains(t, functions, name,
			"production reservation, preflight, and execution must not accept raw traces")
	}
	for _, name := range []string{
		"reserveValidatedPhaseTrace",
		"preflightValidatedTraceOperations",
		"executeValidatedFrozenTrace",
	} {
		require.True(t, functionAcceptsValidatedCarrier(functions[name]),
			"%s must accept *validatedPhaseTrace", name)
	}

	require.Equal(t, []string{"frozen"}, productionStructFieldNames(t, files, "validatedPhaseTrace"))
	fields := productionStructFieldNames(t, files, "coordinatorRound")
	require.NotContains(t, fields, "frozenTrace")
	require.NotContains(t, fields, "executionTicket")
}

func parseTopologyProductionFiles(t *testing.T) []*ast.File {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	entries, err := os.ReadDir(filepath.Dir(currentFile))
	require.NoError(t, err)

	fileSet := token.NewFileSet()
	files := make([]*ast.File, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") ||
			strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(
			fileSet, filepath.Join(filepath.Dir(currentFile), entry.Name()), nil, 0)
		require.NoError(t, err)
		files = append(files, file)
	}
	return files
}

func functionAcceptsValidatedCarrier(function *ast.FuncDecl) bool {
	if function == nil || function.Type.Params == nil {
		return false
	}
	for _, field := range function.Type.Params.List {
		pointer, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		identifier, ok := pointer.X.(*ast.Ident)
		if ok && identifier.Name == "validatedPhaseTrace" {
			return true
		}
	}
	return false
}

func productionStructFieldNames(t *testing.T, files []*ast.File, typeName string) []string {
	t.Helper()
	for _, file := range files {
		for _, declaration := range file.Decls {
			typeDeclaration, ok := declaration.(*ast.GenDecl)
			if !ok || typeDeclaration.Tok != token.TYPE {
				continue
			}
			for _, spec := range typeDeclaration.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok || typeSpec.Name.Name != typeName {
					continue
				}
				structType, ok := typeSpec.Type.(*ast.StructType)
				require.True(t, ok, "%s must remain a struct", typeName)
				var fields []string
				for _, field := range structType.Fields.List {
					for _, name := range field.Names {
						fields = append(fields, name.Name)
					}
				}
				return fields
			}
		}
	}
	t.Fatalf("production type %s not found", typeName)
	return nil
}
