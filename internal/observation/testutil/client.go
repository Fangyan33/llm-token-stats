// Package testutil provides a single-capture view of the batch transport for
// attribution tests. It is not a server compatibility endpoint.
package testutil

import (
	"context"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"google.golang.org/grpc"
	"io"
)

type Client struct{ pb.ObservationServiceClient }

func New(c grpc.ClientConnInterface) *Client { return &Client{pb.NewObservationServiceClient(c)} }

type Stream struct {
	pb.ObservationService_ObserveClient
	batch   uint64
	pending []*pb.ObservationAck
}

func (c *Client) Observe(ctx context.Context, opts ...grpc.CallOption) (*Stream, error) {
	s, err := c.ObservationServiceClient.Observe(ctx, opts...)
	return &Stream{ObservationService_ObserveClient: s}, err
}
func (s *Stream) Send(f *pb.ObservationFrame) error {
	s.batch++
	return s.ObservationService_ObserveClient.Send(&pb.ObservationBatch{BatchSeq: s.batch, Frames: []*pb.CaptureFrame{{CaptureId: 1, Frame: f}}})
}
func (s *Stream) Recv() (*pb.ObservationAck, error) {
	for {
		if len(s.pending) > 0 {
			a := s.pending[0]
			s.pending = s.pending[1:]
			return a, nil
		}
		b, err := s.ObservationService_ObserveClient.Recv()
		if err != nil {
			return nil, err
		}
		for _, a := range b.Acks {
			if a.CaptureId == 1 {
				s.pending = append(s.pending, a.Ack)
			}
		}
	}
}

type FrameServer interface {
	grpc.ServerStream
	Send(*pb.ObservationAck) error
	Recv() (*pb.ObservationFrame, error)
}
type BatchServer struct {
	FrameServer
	seq uint64
}

func (s *BatchServer) Send(b *pb.ObservationAckBatch) error {
	for _, a := range b.Acks {
		if err := s.FrameServer.Send(a.Ack); err != nil {
			return err
		}
	}
	return nil
}
func (s *BatchServer) Recv() (*pb.ObservationBatch, error) {
	for {
		f, err := s.FrameServer.Recv()
		if err != nil {
			return nil, err
		}
		if f == nil {
			return nil, io.EOF
		}
		s.seq++
		return &pb.ObservationBatch{BatchSeq: s.seq, Frames: []*pb.CaptureFrame{{CaptureId: 1, Frame: f}}}, nil
	}
}
