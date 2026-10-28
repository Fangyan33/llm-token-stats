// Package statwindow defines the shared UTC calendar retention boundary.
package statwindow

import "time"

func Day(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
func Month(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// Start retains twelve complete calendar months plus the current month.
func Start(now time.Time) time.Time { return Month(now).AddDate(0, -12, 0) }
