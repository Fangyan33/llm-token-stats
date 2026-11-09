package logging

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Diagnostic preserves an existing public error code while retaining its cause
// for safe logs. Error text and callers' string-based classifications stay stable.
type diagnostic struct {
	code  string
	cause error
}

func (e diagnostic) Error() string             { return e.code }
func (e diagnostic) Unwrap() error             { return e.cause }
func WithCause(code string, cause error) error { return diagnostic{code, cause} }
func errorsCanceled(err error) bool            { return errors.Is(err, context.Canceled) }

// ErrorFields deliberately does not print arbitrary error strings. HTTP URLs,
// SQL values, server responses and credentials may occur inside those strings.
// Known transport errors are decomposed; SQL and Kafka expose protocol codes.
func ErrorFields(err error) []any {
	if err == nil {
		return nil
	}
	fields := []any{"error_type", fmt.Sprintf("%T", err)}
	for depth := 0; err != nil && depth < 12; depth++ {
		switch e := err.(type) {
		case diagnostic:
			fields = append(fields, "error_code", e.code)
			err = e.cause
			continue
		case *url.Error:
			fields = append(fields, "operation", e.Op)
			err = e.Err
			continue
		case *net.OpError:
			fields = append(fields, "network_operation", e.Op, "network", e.Net)
			err = e.Err
			continue
		case *os.PathError:
			fields = append(fields, "file_operation", e.Op)
			err = e.Err
			continue
		case *os.SyscallError:
			fields = append(fields, "syscall", e.Syscall)
			err = e.Err
			continue
		case *net.DNSError:
			return append(fields, "error", "dns_failure", "timeout", e.IsTimeout, "not_found", e.IsNotFound)
		case *mysql.MySQLError:
			return append(fields, "error", "mysql_error", "sql_error_code", int(e.Number), "sql_state", string(e.SQLState[:]))
		case *kerr.Error:
			message := "kafka_protocol_error"
			if known, ok := kerr.ErrorForCode(e.Code).(*kerr.Error); ok {
				message = known.Message
			}
			return append(fields, "error", message, "kafka_error_code", int(e.Code), "retriable", e.Retriable)
		case syscall.Errno:
			return append(fields, "error", e.Error(), "errno", int(e))
		case x509.UnknownAuthorityError:
			return append(fields, "error", "tls_unknown_authority")
		case x509.HostnameError:
			return append(fields, "error", "tls_hostname_mismatch")
		case x509.CertificateInvalidError:
			return append(fields, "error", "tls_certificate_invalid", "certificate_reason", int(e.Reason))
		case *tls.CertificateVerificationError:
			err = e.Err
			continue
		case tls.RecordHeaderError:
			return append(fields, "error", "tls_invalid_record")
		case *json.SyntaxError:
			return append(fields, "error", "invalid_json", "byte_offset", e.Offset)
		case *json.UnmarshalTypeError:
			return append(fields, "error", "json_type_mismatch", "byte_offset", e.Offset)
		}
		switch {
		case errors.Is(err, kgo.ErrRecordTimeout):
			return append(fields, "error", "kafka_record_timeout", "timeout", true)
		case errors.Is(err, kgo.ErrMaxBuffered):
			return append(fields, "error", "kafka_buffer_full")
		case errors.Is(err, kgo.ErrClientClosed):
			return append(fields, "error", "kafka_client_closed")
		case errors.Is(err, context.Canceled):
			return append(fields, "error", "context_canceled")
		case errors.Is(err, context.DeadlineExceeded):
			return append(fields, "error", "deadline_exceeded", "timeout", true)
		case errors.Is(err, io.EOF):
			return append(fields, "error", "eof")
		case errors.Is(err, io.ErrUnexpectedEOF):
			return append(fields, "error", "unexpected_eof")
		case errors.Is(err, driver.ErrBadConn):
			return append(fields, "error", "bad_connection")
		case errors.Is(err, net.ErrClosed):
			return append(fields, "error", "connection_closed")
		}
		var timeout interface{ Timeout() bool }
		if errors.As(err, &timeout) && timeout.Timeout() {
			return append(fields, "error", "network_timeout", "timeout", true)
		}
		if next := errors.Unwrap(err); next != nil {
			err = next
			continue
		}
		return append(fields, "error", "unclassified_error")
	}
	return fields
}
