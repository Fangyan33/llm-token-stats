GOCACHE ?= /tmp/metering-go-cache
GOMODCACHE ?= /tmp/metering-mod
GOPATH ?= /tmp/metering-go
export GOCACHE GOMODCACHE GOPATH
PROTOC ?= protoc
PACKAGE_DIR ?= dist/package
.PHONY: build wasm test generate local-infra-up local-up k8s-test-build k8s-test k8s-clean manifest package summary-test local-summary capacity-build capacity-test collection-regression collection-fault-test
build:
	mkdir -p dist
	CGO_ENABLED=0 go build -trimpath -o dist/observer ./cmd/observer
	CGO_ENABLED=0 go build -trimpath -o dist/consumer ./cmd/consumer
	CGO_ENABLED=0 go build -trimpath -o dist/usage-worker ./cmd/usage-worker
	CGO_ENABLED=0 go build -trimpath -o dist/fixture ./tests/integration/fixture
wasm:
	mkdir -p dist
	cd plugins/gateway-observer && tinygo build -target=wasi -buildmode=wasi-legacy -scheduler=none -o ../../dist/gateway-observer-v1.wasm .
	python3 scripts/check_wasm_abi.py dist/gateway-observer-v1.wasm
test:
	python3 scripts/validate_collection_contracts.py
	python3 -m unittest discover -s tests -p 'test_*.py'
	go test -race ./...
	cd plugins/gateway-observer && go test -race ./...
generate:
	$(PROTOC) --go_out=. --go_opt=module=github.com/Fangyan33/llm-token-stats --go-grpc_out=. --go-grpc_opt=module=github.com/Fangyan33/llm-token-stats api/observation/v1/observation.proto
local-up:
	python3 scripts/local_collection.py up
local-infra-up:
	python3 scripts/local_collection.py infra-up
summary-test:
	go test -tags=integration -count=1 -v ./internal/summaryworker
local-summary: build
	./dist/usage-worker --config configs/usage-worker.local.json --once
k8s-test-build:
	mkdir -p dist
	CGO_ENABLED=0 go test -tags=integration -c -o dist/integration.test ./tests/integration
k8s-test: build wasm k8s-test-build
	python3 scripts/k8s_collection.py test $(K8S_TEST_ARGS)
k8s-clean:
	python3 scripts/cleanup_k8s_collection.py $(K8S_TEST_ARGS)
manifest:
	python3 scripts/collection_manifest.py
package: wasm
	python3 scripts/package_distribution.py --output "$(PACKAGE_DIR)" $(PACKAGE_ARGS)

capacity-build:
	mkdir -p dist
	CGO_ENABLED=0 go build -trimpath -o dist/collection-load ./tests/capacity
capacity-test: build wasm capacity-build
	python3 scripts/collection_capacity.py $(CAPACITY_ARGS)
collection-regression: build wasm capacity-build
	python3 scripts/collection_capacity.py --mode regression $(CAPACITY_ARGS)

collection-fault-test: build wasm capacity-build
	python3 scripts/collection_capacity.py --mode faults $(CAPACITY_ARGS)
