// Copyright 2026
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

//go:build wasmhosttest

package internal

import (
	"context"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the allocator that Envoy actually prefers, as well as the SDK's
// Go allocator fallback. A proxy_on_memory_allocate-only fake misses C pins.
func TestHostBufferOwnership(t *testing.T) {
	wasm := filepath.Join(t.TempDir(), "probe.wasm")
	cmd := exec.Command("tinygo", "build", "-target=wasi", "-buildmode=wasi-legacy", "-scheduler=none", "-o", wasm, "./testdata/host-buffer")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("TinyGo ownership probe: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(wasm)
	if err != nil {
		t.Fatal(err)
	}
	for _, allocator := range []string{"malloc", "proxy_on_memory_allocate"} {
		t.Run(allocator, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfigCompiler().WithMemoryLimitPages(256).WithCloseOnContextDone(true))
			defer r.Close(ctx)
			if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
				t.Fatal(err)
			}
			compiled, err := r.CompileModule(ctx, raw)
			if err != nil {
				t.Fatal(err)
			}
			builders := map[string]wazero.HostModuleBuilder{}
			calls := 0
			for _, f := range compiled.ImportedFunctions() {
				module, name, ok := f.Import()
				if !ok || module == "wasi_snapshot_preview1" {
					continue
				}
				builder := builders[module]
				if builder == nil {
					builder = r.NewHostModuleBuilder(module)
					builders[module] = builder
				}
				results := len(f.ResultTypes())
				builder.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
					switch name {
					case "proxy_get_buffer_bytes", "proxy_get_header_map_value", "proxy_get_property":
						var dataOut, sizeOut uint32
						if name == "proxy_get_property" {
							dataOut, sizeOut = uint32(stack[2]), uint32(stack[3])
						} else {
							dataOut, sizeOut = uint32(stack[3]), uint32(stack[4])
						}
						value, e := m.ExportedFunction(allocator).Call(ctx, 16384)
						if e != nil {
							panic(e)
						}
						data, ok := m.Memory().Read(uint32(value[0]), 16384)
						if !ok {
							panic("buffer bounds")
						}
						for i := range data {
							data[i] = 42
						}
						if !m.Memory().WriteUint32Le(dataOut, uint32(value[0])) || !m.Memory().WriteUint32Le(sizeOut, 16384) {
							panic("output bounds")
						}
						calls++
						stack[0] = 0
						return
					}
					for i := 0; i < results; i++ {
						stack[i] = 0
					}
				}), f.ParamTypes(), f.ResultTypes()).Export(name)
			}
			for _, b := range builders {
				if _, err := b.Instantiate(ctx); err != nil {
					t.Fatal(err)
				}
			}
			m, err := r.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
			if err != nil {
				t.Fatal(err)
			}
			// Unrelated C allocations must keep their C lifetime: adoption may
			// remove only the specific buffer handed to the Go caller.
			unrelated, err := m.ExportedFunction("malloc").Call(ctx, 32)
			if err != nil {
				t.Fatal(err)
			}
			other, ok := m.Memory().Read(uint32(unrelated[0]), 32)
			if !ok {
				t.Fatal("unrelated C allocation bounds")
			}
			for i := range other {
				other[i] = 7
			}
			call := func(name string, args ...uint64) uint64 {
				t.Helper()
				v, e := m.ExportedFunction(name).Call(ctx, args...)
				if e != nil {
					t.Fatal(e)
				}
				if len(v) == 0 {
					return 0
				}
				return v[0]
			}
			initial := call("probe_pins")
			if initial != 1 {
				t.Fatal("C allocation was not pinned", initial)
			}
			for i := 0; i < 128; i++ {
				for kind := uint64(0); kind < 3; kind++ {
					if call("probe_read", kind) != 1 {
						t.Fatal("typed view lost across GC")
					}
					if got := call("probe_pins"); got != initial {
						t.Fatalf("host allocation still C-pinned: %d -> %d", initial, got)
					}
				}
				call("probe_release")
			}
			if call("probe_owned") != 1 {
				t.Fatal("Go-owned buffer damaged")
			}
			call("probe_release")
			other, ok = m.Memory().Read(uint32(unrelated[0]), 32)
			if !ok || other[0] != 7 || other[31] != 7 || call("probe_pins") != initial {
				t.Fatal("unrelated C lifetime changed")
			}
			call("free", unrelated[0])
			if call("probe_pins") != 0 {
				t.Fatal("unrelated C allocation could not be freed")
			}
			if calls != 384 || call("probe_heap") > 1<<20 {
				t.Fatal("repeated host buffers retained", calls)
			}
		})
	}
}
