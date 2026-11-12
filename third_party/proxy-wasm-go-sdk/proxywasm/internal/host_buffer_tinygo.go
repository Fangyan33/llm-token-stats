// Copyright 2026
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build tinygo.wasm && !(custommalloc || wasm_unknown || gc.boehm)

package internal

// TinyGo 0.40.1's exported malloc pins C-owned allocations in runtime.allocs.
// Envoy prefers that export for ABI output buffers. Once an output is wrapped
// in a Go slice/string, that typed value owns its lifetime. Removing only the
// C pin preserves zero-copy access and lets GC collect the buffer after use.
// This matches libc_free in the pinned runtime: it deletes the map entry and
// never immediately reuses the storage. Go-owned/proxy_on_memory_allocate
// buffers have no entry, so adoption is harmless for that allocator as well.
// Keep this adapter tied to the repository's fixed TinyGo runtime/toolchain.

//go:extern runtime.allocs
var hostAllocations map[*byte]uintptr

func adoptHostAllocation(raw *byte) {
	delete(hostAllocations, raw)
}
