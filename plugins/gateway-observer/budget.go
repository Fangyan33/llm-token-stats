package main

import (
	"encoding/binary"
	"strconv"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/Fangyan33/llm-token-stats/pkg/gatewayconfig"
	"github.com/Fangyan33/llm-token-stats/pkg/logsafe"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
)

// SharedData is shared by all worker VMs with the same stable vm_id. Envoy's
// main-thread plugin initialization creates this ledger before cloning workers.
// Never expire an owner's lease: a stalled VM can still own native send buffers.
type podBudget struct {
	key                 string
	limits, owned, used [3]int64
	owner               *root
	failure             []string
}

func newPodBudget(c gatewayconfig.Config, instance string) (*podBudget, error) {
	b := &podBudget{key: "llm-token-stats/pod-budget/" + instance, limits: [3]int64{c.PodInflightBytes, c.PodInflightMessages, c.PodMaxObservations}}
	data, _, err := proxywasm.GetSharedData(b.key)
	if err == types.ErrorStatusNotFound {
		data = make([]byte, 48)
		for i, v := range b.limits {
			binary.LittleEndian.PutUint64(data[i*8:], uint64(v))
		}
		err = proxywasm.SetSharedData(b.key, data, 0)
	}
	if err != nil {
		return nil, configdiag.New("budget_unavailable", "$", "shared_data_"+hostError(err), "readable/writable Pod budget ledger", nil)
	}
	if len(data) != 48 {
		return nil, configdiag.New("budget_unavailable", "$", "invalid_ledger_size", "48-byte Pod budget ledger", nil)
	}
	for i, v := range b.limits {
		if binary.LittleEndian.Uint64(data[i*8:]) != uint64(v) {
			r := configdiag.New("budget_configuration_mismatch", budgetKind(i), "shared_limit_mismatch", strconv.FormatInt(v, 10), nil)
			r.Issues[0].Actual = strconv.FormatUint(binary.LittleEndian.Uint64(data[i*8:]), 10)
			return nil, r
		}
	}
	return b, nil
}

func budgetKind(i int) string {
	return [3]string{"pod_inflight_bytes", "pod_inflight_messages", "pod_max_observations"}[i]
}
func (b *podBudget) failed(reason, operation string, err error) {
	b.failure = []string{"reason", reason, "operation", operation, "host_status", hostError(err)}
	if b.owner != nil {
		b.owner.repeatedLog(logsafe.Warn, "pod_budget_ledger_failed", append(b.failure, "action", "retain_budget")...)
	}
}
func (b *podBudget) logFailure(r *root, event, action string) {
	fields := []string{"reason", "budget_unavailable"}
	if b != nil && len(b.failure) > 0 {
		fields = b.failure
	}
	r.repeatedLog(logsafe.Warn, event, append(append([]string(nil), fields...), "action", action)...)
}

func (b *podBudget) change(delta [3]int64) bool {
	for attempt := 0; attempt < 8; attempt++ {
		data, cas, err := proxywasm.GetSharedData(b.key)
		if err != nil || len(data) != 48 {
			b.failed("shared_data_read_failed", "get_shared_data", err)
			return false
		}
		next := append([]byte(nil), data...)
		for i, d := range delta {
			old := int64(binary.LittleEndian.Uint64(data[24+i*8:]))
			n := old + d
			if n < 0 || n > b.limits[i] {
				b.failure = []string{"reason", "limit_exceeded", "limit_kind", budgetKind(i), "used", strconv.FormatInt(old, 10), "requested", strconv.FormatInt(d, 10), "limit", strconv.FormatInt(b.limits[i], 10)}
				return false
			}
			binary.LittleEndian.PutUint64(next[24+i*8:], uint64(n))
		}
		err = proxywasm.SetSharedData(b.key, next, cas)
		if err == nil {
			if b.owner != nil {
				b.owner.logRecovered("pod_budget_ledger_failed")
			}
			return true
		}
		if err != types.ErrorStatusCasMismatch {
			b.failed("shared_data_write_failed", "set_shared_data", err)
			return false
		}
	}
	b.failed("cas_attempts_exhausted", "set_shared_data", types.ErrorStatusCasMismatch)
	return false
}

func (b *podBudget) take(bytes, messages, observations int64) bool {
	if b == nil {
		return false
	}
	need := [3]int64{bytes, messages, observations}
	units := [3]int64{1 << 20, 1024, 256}
	var claim [3]int64
	for i, n := range need {
		if n < 0 {
			return false
		}
		missing := b.used[i] + n - b.owned[i]
		if missing > 0 {
			unit := min(units[i], b.limits[i])
			claim[i] = (missing + unit - 1) / unit * unit
		}
	}
	if claim != [3]int64{} && !b.change(claim) {
		return false
	}
	for i, n := range need {
		b.owned[i] += claim[i]
		b.used[i] += n
	}
	return true
}
func (b *podBudget) give(bytes, messages, observations int64) {
	if b == nil {
		return
	}
	values := [3]int64{bytes, messages, observations}
	units := [3]int64{1 << 20, 1024, 256}
	var release [3]int64
	for i, n := range values {
		b.used[i] -= n
		if b.used[i] < 0 {
			b.used[i] = 0
		}
		free := b.owned[i] - b.used[i]
		unit := min(units[i], b.limits[i])
		if b.used[i] == 0 {
			release[i] = -free
		} else if free >= 2*unit {
			release[i] = -(free/unit - 1) * unit
		}
	}
	if release != [3]int64{} && b.change(release) {
		for i, n := range release {
			b.owned[i] += n
		}
	}
}
func (b *podBudget) close() {
	if b == nil {
		return
	}
	var delta [3]int64
	for i, n := range b.owned {
		delta[i] = -n
	}
	if b.change(delta) {
		b.owned = [3]int64{}
		b.used = [3]int64{}
	}
}
