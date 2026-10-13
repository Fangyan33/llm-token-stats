package proxywasm

import "github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/internal"

type GrpcCallbacks = internal.GrpcCallbacks

// OpenGrpcStream accepts serialized envoy.config.core.v3.GrpcService.
func OpenGrpcStream(root uint32, upstream []byte, service, method string, cb GrpcCallbacks) (uint32, error) {
	old := internal.ActiveContext()
	if err := SetEffectiveContext(root); err != nil {
		return 0, err
	}
	defer SetEffectiveContext(old)
	md := internal.SerializeMap(nil)
	var token uint32
	st := internal.ProxyGrpcStream(ptr(upstream), len(upstream), internal.StringBytePtr(service), len(service), internal.StringBytePtr(method), len(method), ptr(md), len(md), &token)
	if st != internal.StatusOK {
		return 0, internal.StatusToError(st)
	}
	internal.RegisterGrpc(root, token, cb)
	return token, nil
}
func SendGrpcMessage(token uint32, b []byte, end bool) error {
	var eos uint32
	if end {
		eos = 1
	}
	return internal.StatusToError(internal.ProxyGrpcSend(token, ptr(b), len(b), eos))
}
func CloseGrpcStream(token uint32) error {
	return internal.StatusToError(internal.ProxyGrpcClose(token))
}
func CancelGrpcStream(root, token uint32) error {
	internal.ForgetGrpc(root, token)
	return internal.StatusToError(internal.ProxyGrpcCancel(token))
}
func GetGrpcReceiveBuffer(size int) ([]byte, error) {
	return getBuffer(internal.BufferTypeGrpcReceiveBuffer, 0, size)
}
func ptr(b []byte) *byte {
	if len(b) == 0 {
		return nil
	}
	return &b[0]
}

func CurrentEffectiveContext() uint32 { return internal.ActiveContext() }
