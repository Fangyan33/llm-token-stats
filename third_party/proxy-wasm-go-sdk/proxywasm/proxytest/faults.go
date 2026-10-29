// Copyright 2026
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package proxytest

import "github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/internal"

// Like the emulator itself, failure injection is scoped to a single serial test.
var hostFailures map[string]int

// FailNextHostCalls fails the next n calls with InternalFailure. Supported
// operations: continue_request, effective_context, read_request_body,
// replace_request_body, define_metric, increment_metric, tick_period.
func FailNextHostCalls(operation string, n int) { hostFailures[operation] = n }

func failHostCall(operation string) bool {
	if hostFailures[operation] > 0 {
		hostFailures[operation]--
		return true
	}
	return false
}

type faultHost struct{ *hostEmulator }

func (h faultHost) ProxyContinueStream(s internal.StreamType) internal.Status {
	if s == internal.StreamTypeRequest && failHostCall("continue_request") {
		return internal.StatusInternalFailure
	}
	return h.hostEmulator.ProxyContinueStream(s)
}

func (h faultHost) ProxySetEffectiveContext(id uint32) internal.Status {
	if failHostCall("effective_context") {
		return internal.StatusInternalFailure
	}
	return h.hostEmulator.ProxySetEffectiveContext(id)
}

func (h faultHost) ProxyGetBufferBytes(b internal.BufferType, start, max int, data **byte, size *int) internal.Status {
	if b == internal.BufferTypeHttpRequestBody && failHostCall("read_request_body") {
		return internal.StatusInternalFailure
	}
	return h.hostEmulator.ProxyGetBufferBytes(b, start, max, data, size)
}

func (h faultHost) ProxySetBufferBytes(b internal.BufferType, start, max int, data *byte, size int) internal.Status {
	if b == internal.BufferTypeHttpRequestBody && failHostCall("replace_request_body") {
		return internal.StatusInternalFailure
	}
	return h.hostEmulator.ProxySetBufferBytes(b, start, max, data, size)
}

func (h faultHost) ProxyDefineMetric(t internal.MetricType, name *byte, size int, id *uint32) internal.Status {
	if failHostCall("define_metric") {
		return internal.StatusInternalFailure
	}
	return h.hostEmulator.ProxyDefineMetric(t, name, size, id)
}

func (h faultHost) ProxyIncrementMetric(id uint32, offset int64) internal.Status {
	if failHostCall("increment_metric") {
		return internal.StatusInternalFailure
	}
	return h.hostEmulator.ProxyIncrementMetric(id, offset)
}

func (h faultHost) ProxySetTickPeriodMilliseconds(period uint32) internal.Status {
	if failHostCall("tick_period") {
		return internal.StatusInternalFailure
	}
	return h.hostEmulator.ProxySetTickPeriodMilliseconds(period)
}
