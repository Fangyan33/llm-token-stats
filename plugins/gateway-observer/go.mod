module github.com/Fangyan33/llm-token-stats/plugins/gateway-observer

go 1.25.0

require (
	github.com/Fangyan33/llm-token-stats v0.0.0
	github.com/tetratelabs/proxy-wasm-go-sdk v0.24.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/tetratelabs/wazero v1.7.2 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251029180050-ab9386a59fda // indirect
	google.golang.org/grpc v1.78.0 // indirect
)

replace github.com/tetratelabs/proxy-wasm-go-sdk => ../../third_party/proxy-wasm-go-sdk

replace github.com/Fangyan33/llm-token-stats => ../..
