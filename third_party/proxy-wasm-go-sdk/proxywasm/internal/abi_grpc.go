package internal

type GrpcCallbacks interface {
	OnReceive(token uint32, size int)
	OnClose(token, status uint32)
}
type grpcKey struct{ root, token uint32 }

var grpcCalls = map[grpcKey]GrpcCallbacks{}

func RegisterGrpc(root, token uint32, cb GrpcCallbacks) { grpcCalls[grpcKey{root, token}] = cb }
func ForgetGrpc(root, token uint32)                     { delete(grpcCalls, grpcKey{root, token}) }
func ForgetRootGrpc(root uint32) {
	for k := range grpcCalls {
		if k.root == root {
			delete(grpcCalls, k)
			ProxyGrpcCancel(k.token)
		}
	}
}
func EffectiveContext(id uint32) Status {
	st := ProxySetEffectiveContext(id)
	if st == StatusOK {
		currentState.setActiveContextID(id)
	}
	return st
}
func ActiveContext() uint32 { return currentState.activeContextID }
func grpcCallback(root, token uint32, fn func(GrpcCallbacks)) {
	cb, ok := grpcCalls[grpcKey{root, token}]
	if !ok {
		return
	}
	if _, ok := currentState.pluginContexts[root]; !ok {
		ForgetGrpc(root, token)
		return
	}
	old := currentState.activeContextID
	if EffectiveContext(root) != StatusOK {
		return
	}
	defer EffectiveContext(old)
	fn(cb)
}

//export proxy_on_grpc_receive_initial_metadata
func proxyOnGrpcReceiveInitialMetadata(root, token uint32, count int) {}

//export proxy_on_grpc_receive
func proxyOnGrpcReceive(root, token uint32, size int) {
	grpcCallback(root, token, func(cb GrpcCallbacks) { cb.OnReceive(token, size) })
}

//export proxy_on_grpc_receive_trailing_metadata
func proxyOnGrpcReceiveTrailingMetadata(root, token uint32, count int) {}

//export proxy_on_grpc_close
func proxyOnGrpcClose(root, token, status uint32) {
	grpcCallback(root, token, func(cb GrpcCallbacks) { ForgetGrpc(root, token); cb.OnClose(token, status) })
}
