//go:build tinygo

package main

import (
	"strings"
	"unsafe"
)

// Use only these two WASI calls. Importing os initializes filesystem support in
// TinyGo, which violates the gateway module's deliberately restricted ABI.
//
//go:wasmimport wasi_snapshot_preview1 environ_sizes_get
func loggingEnvironSizes(count, size *uint32) uint32

//go:wasmimport wasi_snapshot_preview1 environ_get
func loggingEnvironGet(pointers *uint32, buffer *byte) uint32

func logLevelEnvironment() string {
	var count, size uint32
	if loggingEnvironSizes(&count, &size) != 0 {
		return "invalid"
	}
	if count == 0 {
		return ""
	}
	if count > 64 || size == 0 || size > 4096 {
		return "invalid"
	}
	pointers, buffer := make([]uint32, count), make([]byte, size)
	if loggingEnvironGet(&pointers[0], &buffer[0]) != 0 {
		return "invalid"
	}
	base := uint32(uintptr(unsafe.Pointer(&buffer[0])))
	for _, pointer := range pointers {
		if pointer < base || pointer-base >= size {
			return "invalid"
		}
		value := buffer[pointer-base:]
		end := 0
		for end < len(value) && value[end] != 0 {
			end++
		}
		if end == len(value) {
			return "invalid"
		}
		if strings.HasPrefix(string(value[:end]), "LOG_LEVEL=") {
			return string(value[len("LOG_LEVEL="):end])
		}
	}
	return ""
}
