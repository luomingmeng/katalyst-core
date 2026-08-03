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

package dynamicpolicy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyPlanningAndAdjustmentContractsHaveZeroProductionReferences(t *testing.T) {
	legacyPluginReconcile := "CPUSet" + "Adjustment" + "Handler"
	legacyPluginReset := "CPUSet" + "Adjustment" + "Disabled" + "Handler"
	forbiddenIdentifiers := map[string]struct{}{
		"new" + "Planning" + "Policy":                         {},
		"planning" + "State":                                  {},
		"planning" + "Context":                                {},
		"planning" + "RampUp" + "Admission":                   {},
		"store" + "Requested":                                 {},
		"err" + "Planning" + "State" + "Persistence":          {},
		"commit" + "Runtime" + "Target":                       {},
		"plan" + "With" + "Owned" + "Target":                  {},
		"run" + "CPUSet" + "Adjustment" + "Handlers":          {},
		"cpuSet" + "Adjustment" + "Handlers":                  {},
		"Register" + "CPUSet" + "Adjustment" + "Handler":      {},
		"run" + "CPUSet" + "Adjustment" + "HandlersWithState": {},
		"run" + "Registered" + "CPUSet" + "Adjustment" +
			"Handlers": {},
		"materialize" + "CPUSet" + "Target":                {},
		"remove" + "Pod":                                   {},
		"remove" + "Container":                             {},
		"Publish" + "Applied" + "Reclaim":                  {},
		"Latest" + "Applied" + "Reclaim":                   {},
		"Committed" + "Raw":                                {},
		"committed" + "Raw":                                {},
		"Apply" + "Transient" + "Protected" + "NonReclaim": {},
	}
	forbiddenTypes := map[string]struct{}{
		legacyPluginReconcile:         {},
		legacyPluginReconcile + "Ctx": {},
	}
	forbiddenBulkheadPluginMethods := map[string]struct{}{
		legacyPluginReconcile: {},
		legacyPluginReset:     {},
	}
	forbiddenManagerMethods := map[string]struct{}{
		"Ap" + "ply": {},
		"Run" + "CPUSet" + "Adjustment" + "Handlers": {},
		"Publish" + "Applied" + "Reclaim":            {},
		"Latest" + "Applied" + "Reclaim":             {},
	}
	for _, legacyFile := range []string{
		"cpuset_" + "adjustment_" + "handler.go",
		filepath.Join("util", "cpuset_"+"adjustment.go"),
	} {
		_, err := os.Stat(legacyFile)
		require.ErrorIs(t, err, os.ErrNotExist, legacyFile)
	}

	fset := token.NewFileSet()
	err := filepath.Walk(".", func(path string, info os.FileInfo, walkErr error) error {
		require.NoError(t, walkErr)
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, filepath.Clean(path), nil, 0)
		require.NoError(t, parseErr, path)
		ast.Inspect(file, func(node ast.Node) bool {
			if assignment, ok := node.(*ast.AssignStmt); ok {
				for _, lhs := range assignment.Lhs {
					selector, ok := lhs.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != "state" {
						continue
					}
					receiver, ok := selector.X.(*ast.Ident)
					if ok && receiver.Name == "p" {
						t.Errorf("%s still temporarily replaces p.state", path)
					}
				}
			}
			if typeSpec, ok := node.(*ast.TypeSpec); ok {
				if _, found := forbiddenTypes[typeSpec.Name.Name]; found {
					t.Errorf("%s still declares legacy adjustment type %q", path, typeSpec.Name.Name)
				}
			}
			if declaration, ok := node.(*ast.FuncDecl); ok {
				if _, found := forbiddenBulkheadPluginMethods[declaration.Name.Name]; found {
					t.Errorf("%s still declares legacy Bulkhead plugin method %q", path, declaration.Name.Name)
				}
			}
			if iface, ok := node.(*ast.InterfaceType); ok {
				for _, field := range iface.Methods.List {
					for _, name := range field.Names {
						if _, found := forbiddenBulkheadPluginMethods[name.Name]; found {
							t.Errorf("%s still declares legacy Bulkhead interface method %q", path, name.Name)
						}
					}
				}
			}
			if selector, ok := node.(*ast.SelectorExpr); ok {
				if _, found := forbiddenBulkheadPluginMethods[selector.Sel.Name]; found {
					t.Errorf("%s still calls legacy Bulkhead plugin method %q", path, selector.Sel.Name)
				}
			}
			if declaration, ok := node.(*ast.FuncDecl); ok && declaration.Recv != nil &&
				len(declaration.Recv.List) == 1 && isManagerReceiver(declaration.Recv.List[0].Type) {
				if _, found := forbiddenManagerMethods[declaration.Name.Name]; found {
					t.Errorf("%s still declares legacy Manager entrypoint %q", path, declaration.Name.Name)
				}
			}
			ident, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if _, found := forbiddenIdentifiers[ident.Name]; found {
				t.Errorf("%s still contains forbidden legacy identifier %q", path, ident.Name)
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
}

func isManagerReceiver(expr ast.Expr) bool {
	switch receiver := expr.(type) {
	case *ast.Ident:
		return receiver.Name == "Manager"
	case *ast.StarExpr:
		ident, ok := receiver.X.(*ast.Ident)
		return ok && ident.Name == "Manager"
	default:
		return false
	}
}

func TestTargetMutationEditorIsNarrowAndDoesNotClonePolicy(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy_transaction.go", nil, 0)
	require.NoError(t, err)

	foundEditor := false
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ImportSpec:
			if n.Path.Value == `"reflect"` {
				t.Error("policy_transaction.go must not import reflect")
			}
		case *ast.TypeSpec:
			if n.Name.Name != "targetMutationEditor" {
				return true
			}
			foundEditor = true
			editor, ok := n.Type.(*ast.StructType)
			require.True(t, ok)
			require.Len(t, editor.Fields.List, 1, "editor must contain only its owned target")
			for _, field := range editor.Fields.List {
				if selector, ok := field.Type.(*ast.StarExpr); ok {
					if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == "DynamicPolicy" {
						t.Error("targetMutationEditor must not retain a DynamicPolicy")
					}
				}
			}
		case *ast.CallExpr:
			if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == "new" && len(n.Args) == 1 {
				if arg, ok := n.Args[0].(*ast.Ident); ok && arg.Name == "DynamicPolicy" {
					t.Error("policy_transaction.go must not allocate a DynamicPolicy copy")
				}
			}
		case *ast.SelectorExpr:
			if ident, ok := n.X.(*ast.Ident); ok && ident.Name == "reflect" {
				t.Error("policy_transaction.go must not use reflect")
			}
			if n.Sel.Name == "policy" {
				if ident, ok := n.X.(*ast.Ident); ok && ident.Name == "editor" {
					t.Error("mutation helpers must use editor/target explicitly, not editor.policy")
				}
			}
		}
		return true
	})
	require.True(t, foundEditor)
}

func TestMutationProductionCodeDoesNotUseReflectOrCopyDynamicPolicy(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, filepath.Clean(name), nil, 0)
		require.NoError(t, parseErr, name)
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ImportSpec:
				if n.Path.Value == `"reflect"` {
					t.Errorf("%s must not use reflect to synthesize a mutation policy", name)
				}
			case *ast.AssignStmt:
				for _, rhs := range n.Rhs {
					unary, ok := rhs.(*ast.StarExpr)
					if !ok {
						continue
					}
					ident, ok := unary.X.(*ast.Ident)
					if ok && (ident.Name == "p" || strings.Contains(strings.ToLower(ident.Name), "policy")) {
						t.Errorf("%s must not shallow-copy DynamicPolicy through *%s", name, ident.Name)
					}
				}
			}
			return true
		})
	}
}

func TestTransactionPlansHaveNoExternalSideEffects(t *testing.T) {
	forbiddenSelectors := map[string]struct{}{
		"AllocateAccompanyResource":     {},
		"ReleaseAccompanyResource":      {},
		"AddContainer":                  {},
		"RemovePod":                     {},
		"StoreInt64":                    {},
		"StoreFloat64":                  {},
		"ApplyCgroupConfigs":            {},
		"ApplyCgroupConfigsWithContext": {},
		"PrepareDurableTarget":          {},
		"CommitTarget":                  {},
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, filepath.Clean(name), nil, 0)
		require.NoError(t, parseErr, name)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "transact" {
				return true
			}
			for _, arg := range call.Args {
				plan, ok := arg.(*ast.FuncLit)
				if !ok {
					continue
				}
				ast.Inspect(plan.Body, func(planNode ast.Node) bool {
					sideEffect, ok := planNode.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if _, forbidden := forbiddenSelectors[sideEffect.Sel.Name]; forbidden {
						t.Errorf("%s transaction plan contains external side effect %s", name, sideEffect.Sel.Name)
					}
					return true
				})
			}
			return true
		})
	}
}

func TestRemovePodTransactionHelperHasNoObservabilitySideEffects(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy.go", nil, 0)
	require.NoError(t, err)

	helperBodies := make(map[string]*ast.BlockStmt)
	var removePodBody *ast.BlockStmt
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		helperBodies[fn.Name.Name] = fn.Body
		if fn.Name.Name == "RemovePod" {
			removePodBody = fn.Body
		}
	}
	require.NotNil(t, removePodBody)

	forbiddenSelectors := map[string]struct{}{
		"StoreInt64":   {},
		"StoreFloat64": {},
		"InfoS":        {},
		"Infof":        {},
		"ErrorS":       {},
		"Errorf":       {},
	}
	foundHelper := false
	ast.Inspect(removePodBody, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "transact" && selector.Sel.Name != "transactWithPostCommit") {
			return true
		}
		for _, arg := range call.Args {
			plan, ok := arg.(*ast.FuncLit)
			if !ok {
				continue
			}
			ast.Inspect(plan.Body, func(planNode ast.Node) bool {
				helperCall, ok := planNode.(*ast.CallExpr)
				if !ok {
					return true
				}
				helperSelector, ok := helperCall.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				receiver, receiverOK := helperSelector.X.(*ast.Ident)
				if !receiverOK || receiver.Name != "p" {
					return true
				}
				helperBody := helperBodies[helperSelector.Sel.Name]
				if helperBody == nil {
					return true
				}
				foundHelper = true
				ast.Inspect(helperBody, func(helperNode ast.Node) bool {
					if _, forbidden := helperNode.(*ast.DeferStmt); forbidden {
						t.Errorf("%s must not defer observability side effects", helperSelector.Sel.Name)
					}
					sideEffect, ok := helperNode.(*ast.SelectorExpr)
					if ok {
						if _, forbidden := forbiddenSelectors[sideEffect.Sel.Name]; forbidden {
							t.Errorf("%s contains observability side effect %s",
								helperSelector.Sel.Name, sideEffect.Sel.Name)
						}
					}
					return true
				})
				return true
			})
		}
		return true
	})
	require.True(t, foundHelper, "RemovePod transaction helper must remain statically inspectable")
}

func TestProductionHasNoDefaultStateMutationWrappers(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, filepath.Clean(name), nil, 0)
		require.NoError(t, parseErr, name)
		ast.Inspect(file, func(node ast.Node) bool {
			fn, ok := node.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				return true
			}
			if len(fn.Body.List) != 1 {
				return false
			}
			forwardsToExplicitTarget := false
			usesDefaultState := false
			ast.Inspect(fn.Body, func(bodyNode ast.Node) bool {
				switch n := bodyNode.(type) {
				case *ast.SelectorExpr:
					if ident, ok := n.X.(*ast.Ident); ok && ident.Name == "p" && n.Sel.Name == "state" {
						usesDefaultState = true
					}
				case *ast.CallExpr:
					selector, ok := n.Fun.(*ast.SelectorExpr)
					if ok && (strings.HasSuffix(selector.Sel.Name, "OnTarget") ||
						strings.HasSuffix(selector.Sel.Name, "ForTarget")) {
						forwardsToExplicitTarget = true
					}
				}
				return true
			})
			lowerName := strings.ToLower(fn.Name.Name)
			isMutation := strings.HasPrefix(lowerName, "allocate") ||
				strings.HasPrefix(lowerName, "apply") ||
				strings.HasPrefix(lowerName, "clean") ||
				strings.HasPrefix(lowerName, "commit") ||
				strings.HasPrefix(lowerName, "create") ||
				strings.HasPrefix(lowerName, "delete") ||
				strings.HasPrefix(lowerName, "put") ||
				strings.HasPrefix(lowerName, "select") ||
				strings.HasPrefix(lowerName, "update") ||
				strings.HasPrefix(lowerName, "write") ||
				strings.HasPrefix(lowerName, "adjust")
			if isMutation && usesDefaultState && forwardsToExplicitTarget {
				t.Errorf("%s contains legacy default-state mutation wrapper %s", name, fn.Name.Name)
			}
			return false
		})
	}
}

func TestRestoreBaseOrBlockUsesOnlyMaterializeEntry(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy_lifecycle.go", nil, 0)
	require.NoError(t, err)

	legacyRegisteredRunner := "run" + "Registered" + "CPUSet" + "Adjustment" + "Handlers"
	legacyRunner := "run" + "CPUSet" + "Adjustment" + "Handlers"
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		fn, ok := node.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "restoreBaseOrBlock" {
			return true
		}
		found = true
		materializeCalls := 0
		ast.Inspect(fn.Body, func(bodyNode ast.Node) bool {
			call, ok := bodyNode.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch {
			case selector.Sel.Name == "materialize":
				materializeCalls++
			case selector.Sel.Name == legacyRegisteredRunner || selector.Sel.Name == legacyRunner:
				t.Errorf("restoreBaseOrBlock must not use legacy handler fallback %s", selector.Sel.Name)
			}
			return true
		})
		require.Equal(t, 1, materializeCalls, "restoreBaseOrBlock must have one materialization entry")
		return false
	})
	require.True(t, found)
}

func TestBootstrapPoolsUseOneOwnedTargetTransaction(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy.go", nil, 0)
	require.NoError(t, err)

	var constructor, bootstrap *ast.FuncDecl
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch fn.Name.Name {
		case "NewDynamicPolicy":
			constructor = fn
		case "bootstrapPools":
			bootstrap = fn
		}
	}
	require.NotNil(t, constructor)
	require.NotNil(t, bootstrap)

	forbiddenConstructorCalls := map[string]struct{}{
		"cleanPoolsOnTarget":        {},
		"initReservePool":           {},
		"initReclaimPool":           {},
		"initInterruptPool":         {},
		"initReservePoolOnTarget":   {},
		"initReclaimPoolOnTarget":   {},
		"initInterruptPoolOnTarget": {},
	}
	bootstrapCalls := 0
	ast.Inspect(constructor.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selector.Sel.Name == "bootstrapPools" {
			bootstrapCalls++
		}
		if _, forbidden := forbiddenConstructorCalls[selector.Sel.Name]; forbidden {
			t.Errorf("NewDynamicPolicy must not call bootstrap helper %s outside the transaction", selector.Sel.Name)
		}
		return true
	})
	require.Equal(t, 1, bootstrapCalls)

	transactionCalls := 0
	ast.Inspect(bootstrap.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "transactBootstrap" {
			transactionCalls++
		}
		return true
	})
	require.Equal(t, 1, transactionCalls)
}

func TestBootstrapPoolHelpersDoNotAccessPolicyState(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy.go", nil, 0)
	require.NoError(t, err)

	required := map[string]bool{
		"cleanPoolsOnTarget":        false,
		"initReservePoolOnTarget":   false,
		"initReclaimPoolOnTarget":   false,
		"initInterruptPoolOnTarget": false,
	}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if _, tracked := required[fn.Name.Name]; !tracked {
			continue
		}
		required[fn.Name.Name] = true
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "state" {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if ok && receiver.Name == "p" {
				t.Errorf("%s must only mutate its explicit owned target", fn.Name.Name)
			}
			return true
		})
	}
	for name, found := range required {
		require.True(t, found, "missing bootstrap owned-target helper %s", name)
	}
}

func TestAdvisorSnapshotAndValidationAreBoundToOwnedTransactionBase(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy_advisor_handler.go", nil, 0)
	require.NoError(t, err)

	functions := make(map[string]*ast.FuncDecl)
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if ok && fn.Body != nil {
			functions[fn.Name.Name] = fn
		}
	}

	requestBuilder := functions["createGetAdviceRequestForTarget"]
	require.NotNil(t, requestBuilder, "request builder must accept the owned transaction base explicitly")
	ast.Inspect(requestBuilder.Body, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "state" {
			return true
		}
		receiver, ok := selector.X.(*ast.Ident)
		if ok && receiver.Name == "p" {
			t.Error("advisor request builder must not read p.state")
		}
		return true
	})

	assertFunctionCalls := func(functionName string, requiredCalls ...string) {
		t.Helper()
		fn := functions[functionName]
		require.NotNil(t, fn)
		found := make(map[string]bool, len(requiredCalls))
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch called := call.Fun.(type) {
			case *ast.Ident:
				found[called.Name] = true
			case *ast.SelectorExpr:
				found[called.Sel.Name] = true
			}
			return true
		})
		for _, required := range requiredCalls {
			require.True(t, found[required], "%s must call %s", functionName, required)
		}
	}

	assertCallsInsideTransaction := func(functionName, transactionName string, requiredCalls ...string) {
		t.Helper()
		fn := functions[functionName]
		require.NotNil(t, fn)

		found := make(map[string]bool, len(requiredCalls))
		transactionCalls := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != transactionName {
				return true
			}
			transactionCalls++
			for _, arg := range call.Args {
				plan, ok := arg.(*ast.FuncLit)
				if !ok {
					continue
				}
				ast.Inspect(plan.Body, func(planNode ast.Node) bool {
					planCall, ok := planNode.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch called := planCall.Fun.(type) {
					case *ast.Ident:
						found[called.Name] = true
					case *ast.SelectorExpr:
						found[called.Sel.Name] = true
					}
					return true
				})
			}
			return true
		})
		require.Equal(t, 1, transactionCalls, "%s must use exactly one %s call", functionName, transactionName)
		for _, required := range requiredCalls {
			require.True(t, found[required], "%s must call %s inside %s", functionName, required, transactionName)
		}
	}

	assertFunctionCalls("prepareCurrentGetAdviceRequestLocked",
		"PrepareDurableTarget", "createGetAdviceRequestForTarget", "NewCPUAdvisorValidator", "ValidateRequest")
	assertFunctionCalls("prepareGetAdviceRequest",
		"prepareCurrentGetAdviceRequestLocked", "normalizedGetAdviceRequestHash")
	assertFunctionCalls("getAdviceFromAdvisor", "prepareGetAdviceRequest", "GetAdvice", "transactIfAdviceFresh")
	assertCallsInsideTransaction("getAdviceFromAdvisor", "transactIfAdviceFresh",
		"NewCPUAdvisorValidator", "Validate")
	assertCallsInsideTransaction("allocateByCPUAdvisor", "transact",
		"NewCPUAdvisorValidator", "ValidateRequest", "Validate")
}

func TestAllocateOwnedTargetDefersNoCompletionOrFailureLogs(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy.go", nil, 0)
	require.NoError(t, err)

	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "allocateOnOwnedTarget" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if _, ok := node.(*ast.DeferStmt); ok {
				t.Error("allocateOnOwnedTarget must not defer completion or failure observability")
			}
			return true
		})
		return
	}
	t.Fatal("allocateOnOwnedTarget not found")
}

func TestTransactionRPCAndWorkerBoundariesLogOutcomeAfterTransact(t *testing.T) {
	testCases := []struct {
		file     string
		function string
	}{
		{file: "policy.go", function: "Allocate"},
		{file: "policy_irq_tuner.go", function: "SetExclusiveIRQCPUSet"},
	}

	for _, tc := range testCases {
		file, err := parser.ParseFile(token.NewFileSet(), tc.file, nil, 0)
		require.NoError(t, err)

		var boundary *ast.FuncDecl
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if ok && fn.Name.Name == tc.function {
				boundary = fn
				break
			}
		}
		require.NotNil(t, boundary, "%s:%s", tc.file, tc.function)

		var transactEnd token.Pos
		ast.Inspect(boundary.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && (selector.Sel.Name == "transact" || selector.Sel.Name == "transactWithPostCommit") {
				transactEnd = call.End()
			}
			return true
		})
		require.NotEqual(t, token.NoPos, transactEnd, "%s:%s must transact", tc.file, tc.function)

		successAfterTransact := false
		failureAfterTransact := false
		ast.Inspect(boundary.Body, func(node ast.Node) bool {
			if _, nestedPlan := node.(*ast.FuncLit); nestedPlan {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok || call.Pos() <= transactEnd {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if strings.HasPrefix(selector.Sel.Name, "Info") {
				successAfterTransact = true
			}
			if strings.HasPrefix(selector.Sel.Name, "Error") {
				failureAfterTransact = true
			}
			return true
		})
		require.True(t, successAfterTransact, "%s:%s must log success after transact returns nil", tc.file, tc.function)
		require.True(t, failureAfterTransact, "%s:%s must log failure at the boundary", tc.file, tc.function)
	}
}

func TestSystemExclusivePoolWorkerSuccessLogIsGuardedByNilTransactionError(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "policy_async_handler.go", nil, 0)
	require.NoError(t, err)

	var worker *ast.FuncDecl
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if ok && fn.Name.Name == "syncSystemExclusivePool" {
			worker = fn
			break
		}
	}
	require.NotNil(t, worker)

	foundGuard := false
	ast.Inspect(worker.Body, func(node ast.Node) bool {
		deferStmt, ok := node.(*ast.DeferStmt)
		if !ok {
			return true
		}
		deferred, ok := deferStmt.Call.Fun.(*ast.FuncLit)
		if !ok {
			return true
		}
		for _, statement := range deferred.Body.List {
			ifStmt, ok := statement.(*ast.IfStmt)
			if !ok || ifStmt.Else == nil {
				continue
			}
			condition, ok := ifStmt.Cond.(*ast.BinaryExpr)
			if !ok || condition.Op != token.NEQ {
				continue
			}
			left, leftOK := condition.X.(*ast.Ident)
			right, rightOK := condition.Y.(*ast.Ident)
			if !leftOK || !rightOK || left.Name != "err" || right.Name != "nil" {
				continue
			}

			failureLogsError := false
			successLogsInfo := false
			ast.Inspect(ifStmt.Body, func(branchNode ast.Node) bool {
				call, ok := branchNode.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && strings.HasPrefix(selector.Sel.Name, "Error") {
					failureLogsError = true
				}
				return true
			})
			ast.Inspect(ifStmt.Else, func(branchNode ast.Node) bool {
				call, ok := branchNode.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && strings.HasPrefix(selector.Sel.Name, "Info") {
					successLogsInfo = true
				}
				return true
			})
			foundGuard = failureLogsError && successLogsInfo
		}
		return true
	})

	require.True(t, foundGuard,
		"syncSystemExclusivePool must log error when err != nil and success only in the else branch")
}
