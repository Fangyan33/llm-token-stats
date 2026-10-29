// Copyright 2026
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package proxywasm

import "github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/internal"

// TryDefineCounterMetric returns host errors instead of panicking. Telemetry
// callers may discard the error without interrupting business processing.
func TryDefineCounterMetric(name string) (MetricCounter, error) {
	var id uint32
	err := internal.StatusToError(internal.ProxyDefineMetric(internal.MetricTypeCounter, internal.StringBytePtr(name), len(name), &id))
	return MetricCounter(id), err
}

// TryDefineGaugeMetric is the non-panicking counterpart of DefineGaugeMetric.
func TryDefineGaugeMetric(name string) (MetricGauge, error) {
	var id uint32
	err := internal.StatusToError(internal.ProxyDefineMetric(internal.MetricTypeGauge, internal.StringBytePtr(name), len(name), &id))
	return MetricGauge(id), err
}

// TryIncrement is the non-panicking counterpart of Increment.
func (m MetricCounter) TryIncrement(offset uint64) error {
	return internal.StatusToError(internal.ProxyIncrementMetric(uint32(m), int64(offset)))
}

// TryAdd is the non-panicking counterpart of Add.
func (m MetricGauge) TryAdd(offset int64) error {
	return internal.StatusToError(internal.ProxyIncrementMetric(uint32(m), offset))
}
