package logging

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaHooks sees connection and group failures which franz-go may retry without
// ever returning them from PollRecords. No Kafka request/record is inspected.
type KafkaHooks struct{}

func (KafkaHooks) OnBrokerConnect(meta kgo.BrokerMetadata, elapsed time.Duration, _ net.Conn, err error) {
	scope := "kafka_connect/" + net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port)))
	fields := []any{"broker_host", meta.Host, "broker_port", int(meta.Port), "duration_ms", elapsed.Milliseconds()}
	if err != nil {
		Failure("kafka_connect_failed", scope, err, fields...)
	} else {
		Recovered(scope, "kafka_connection_recovered", fields...)
		Debug("kafka_connection_established", fields...)
	}
}
func (KafkaHooks) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, e kgo.BrokerE2E) {
	scope := "kafka_request/" + net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port))) + "/" + strconv.Itoa(int(key))
	fields := []any{"broker_host", meta.Host, "broker_port", int(meta.Port), "kafka_api_key", int(key), "duration_ms", e.DurationE2E().Milliseconds()}
	if err := e.Err(); err != nil {
		if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
			return
		}
		Failure("kafka_request_failed", scope, err, fields...)
	} else {
		Recovered(scope, "kafka_requests_recovered", fields...)
	}
}
func (KafkaHooks) OnGroupManageError(err error) { Failure("kafka_group_failed", "kafka_group", err) }

var _ kgo.HookBrokerConnect = KafkaHooks{}
var _ kgo.HookBrokerE2E = KafkaHooks{}
var _ kgo.HookGroupManageError = KafkaHooks{}
