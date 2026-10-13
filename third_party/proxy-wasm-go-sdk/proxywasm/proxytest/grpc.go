package proxytest

import "github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/internal"

type GrpcCall struct {
	Token           uint32
	Upstream        []byte
	Service, Method string
	Messages        [][]byte
	End             []bool
	Cancelled       bool
}

var grpcNext uint32
var grpcRecords = map[uint32]*GrpcCall{}
var grpcBuffer []byte

func (h *hostEmulator) ProxyGrpcStream(u *byte, ul int, s *byte, sl int, m *byte, ml int, md *byte, mdl int, t *uint32) internal.Status {
	grpcNext++
	*t = grpcNext
	grpcRecords[*t] = &GrpcCall{Token: *t, Upstream: append([]byte{}, internal.RawBytePtrToByteSlice(u, ul)...), Service: string(internal.RawBytePtrToByteSlice(s, sl)), Method: string(internal.RawBytePtrToByteSlice(m, ml))}
	return internal.StatusOK
}
func (h *hostEmulator) ProxyGrpcSend(t uint32, b *byte, n int, e uint32) internal.Status {
	c := grpcRecords[t]
	if c == nil || c.Cancelled {
		return internal.StatusNotFound
	}
	c.Messages = append(c.Messages, append([]byte{}, internal.RawBytePtrToByteSlice(b, n)...))
	c.End = append(c.End, e == 1)
	return internal.StatusOK
}
func (h *hostEmulator) ProxyGrpcClose(t uint32) internal.Status { return h.ProxyGrpcCancel(t) }
func (h *hostEmulator) ProxyGrpcCancel(t uint32) internal.Status {
	if c := grpcRecords[t]; c != nil {
		c.Cancelled = true
	}
	return internal.StatusOK
}
func GrpcCalls() []*GrpcCall {
	out := []*GrpcCall{}
	for _, c := range grpcRecords {
		out = append(out, c)
	}
	return out
}
func ResetGrpc() { grpcRecords = map[uint32]*GrpcCall{}; grpcNext = 0; grpcBuffer = nil }
func EmitGrpcReceive(root, token uint32, b []byte) {
	grpcBuffer = b
	internal.GrpcReceiveForTest(root, token, len(b))
}
func EmitGrpcClose(root, token, status uint32) { internal.GrpcCloseForTest(root, token, status) }
