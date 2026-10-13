//go:build !tinygo

package internal

type GrpcHost interface {
	ProxyGrpcStream(upstream *byte, upstreamLen int, service *byte, serviceLen int, method *byte, methodLen int, metadata *byte, metadataLen int, token *uint32) Status
	ProxyGrpcSend(token uint32, data *byte, size int, end uint32) Status
	ProxyGrpcClose(token uint32) Status
	ProxyGrpcCancel(token uint32) Status
}

func ProxyGrpcStream(u *byte, ul int, s *byte, sl int, m *byte, ml int, md *byte, mdl int, t *uint32) Status {
	h, ok := currentHost.(GrpcHost)
	if !ok {
		return StatusUnimplemented
	}
	return h.ProxyGrpcStream(u, ul, s, sl, m, ml, md, mdl, t)
}
func ProxyGrpcSend(t uint32, b *byte, n int, e uint32) Status {
	h, ok := currentHost.(GrpcHost)
	if !ok {
		return StatusUnimplemented
	}
	return h.ProxyGrpcSend(t, b, n, e)
}
func ProxyGrpcClose(t uint32) Status {
	h, ok := currentHost.(GrpcHost)
	if !ok {
		return StatusUnimplemented
	}
	return h.ProxyGrpcClose(t)
}
func ProxyGrpcCancel(t uint32) Status {
	h, ok := currentHost.(GrpcHost)
	if !ok {
		return StatusUnimplemented
	}
	return h.ProxyGrpcCancel(t)
}
func GrpcReceiveForTest(root, token uint32, size int) { proxyOnGrpcReceive(root, token, size) }
func GrpcCloseForTest(root, token, status uint32)     { proxyOnGrpcClose(root, token, status) }
