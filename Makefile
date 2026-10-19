GOCACHE ?= /tmp/metering-go-cache
GOMODCACHE ?= /tmp/metering-mod
GOPATH ?= /tmp/metering-go
export GOCACHE GOMODCACHE GOPATH
PROTOC ?= protoc
PACKAGE_DIR ?= dist/package
.PHONY: build wasm test generate local-infra-up local-up local-test k8s-test k8s-clean manifest package
build:
	mkdir -p dist
	CGO_ENABLED=0 go build -trimpath -o dist/observer ./cmd/observer
	CGO_ENABLED=0 go build -trimpath -o dist/consumer ./cmd/consumer
	CGO_ENABLED=0 go build -trimpath -o dist/fixture ./tests/integration/fixture
wasm:
	mkdir -p dist
	cd plugins/gateway-observer && tinygo build -target=wasi -scheduler=none -o ../../dist/gateway-observer-v1.wasm .
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
local-test:
	go test -tags=integration -v ./tests/integration
k8s-test: build wasm
	python3 scripts/k8s_collection.py test $(K8S_TEST_ARGS)
k8s-clean:
	python3 scripts/cleanup_k8s_collection.py $(K8S_TEST_ARGS)
manifest:
	python3 scripts/collection_manifest.py
package: wasm
	python3 scripts/package_distribution.py --output "$(PACKAGE_DIR)" $(PACKAGE_ARGS)
