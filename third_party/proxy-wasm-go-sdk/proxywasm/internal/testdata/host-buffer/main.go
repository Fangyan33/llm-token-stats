// Copyright 2026
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.
package main

import (
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/internal"
	"runtime"
)

//go:extern runtime.allocs
var pinned map[*byte]uintptr
var held []byte
var text string

func main() {}

//export probe_read
func read(kind uint32) uint32 {
	var err error
	switch kind {
	case 0:
		held, err = proxywasm.GetHttpResponseBody(0, 16384)
	case 1:
		text, err = proxywasm.GetHttpResponseHeader("synthetic")
	case 2:
		held, err = proxywasm.GetProperty([]string{"synthetic"})
	}
	if err != nil {
		return 0
	}
	runtime.GC()
	if kind == 1 {
		if len(text) != 16384 || text[0] != 42 || text[len(text)-1] != 42 {
			return 0
		}
	} else {
		if len(held) != 16384 || held[0] != 42 || held[len(held)-1] != 42 {
			return 0
		}
		// A second typed view must not double-free or alter ownership.
		text = internal.RawBytePtrToString(&held[0], len(held))
		runtime.GC()
		if text[0] != 42 {
			return 0
		}
	}
	return 1
}

//export probe_release
func release() { held = nil; text = ""; runtime.GC() }

//export probe_pins
func pins() uint32 { return uint32(len(pinned)) }

//export probe_owned
func owned() uint32 {
	data := make([]byte, 64)
	data[0] = 42
	held = internal.RawBytePtrToByteSlice(&data[0], len(data))
	text = internal.RawBytePtrToString(&data[0], len(data))
	_ = internal.RawBytePtrToByteSlice(nil, 0)
	_ = internal.RawBytePtrToString(nil, 0)
	runtime.GC()
	if held[0] != 42 || text[0] != 42 {
		return 0
	}
	return 1
}

//export probe_heap
func heap() uint64 { var m runtime.MemStats; runtime.ReadMemStats(&m); return m.HeapAlloc }
