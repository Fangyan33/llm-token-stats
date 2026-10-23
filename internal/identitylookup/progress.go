package identitylookup

import "context"

type failureObserverKey struct{}

// WithFailureObserver reports a definitive failure before diagnostic body
// processing finishes. The observer must return promptly and must not perform
// I/O. This notification does not end Resolve's ownership of its borrowed token.
// Only fixed, classified errors are reported; no response or credential escapes.
func WithFailureObserver(ctx context.Context, observer func(error)) context.Context {
	return context.WithValue(ctx, failureObserverKey{}, observer)
}

func reportFailure(ctx context.Context, err error) {
	if observer, ok := ctx.Value(failureObserverKey{}).(func(error)); ok && observer != nil {
		observer(err)
	}
}
