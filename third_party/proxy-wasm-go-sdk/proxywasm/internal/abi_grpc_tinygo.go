//go:build tinygo

package internal

//export proxy_grpc_stream
func ProxyGrpcStream(upstream *byte, upstreamLen int, service *byte, serviceLen int, method *byte, methodLen int, metadata *byte, metadataLen int, token *uint32) Status

//export proxy_grpc_send
func ProxyGrpcSend(token uint32, data *byte, size int, end uint32) Status

//export proxy_grpc_close
func ProxyGrpcClose(token uint32) Status

//export proxy_grpc_cancel
func ProxyGrpcCancel(token uint32) Status
