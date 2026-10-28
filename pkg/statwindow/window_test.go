package statwindow

import (
	"testing"
	"time"
)

func TestCalendarWindow(t *testing.T) {
	for _, tc := range []struct{ now, start string }{{"2026-10-28T23:59:59Z", "2025-10-01"}, {"2026-11-01T00:00:00Z", "2025-11-01"}, {"2024-02-29T12:00:00Z", "2023-02-01"}, {"2025-01-01T00:00:00+08:00", "2023-12-01"}} {
		t.Run(tc.now, func(t *testing.T) {
			now, e := time.Parse(time.RFC3339, tc.now)
			if e != nil {
				t.Fatal(e)
			}
			if got := Start(now).Format("2006-01-02"); got != tc.start {
				t.Fatalf("%s != %s", got, tc.start)
			}
		})
	}
}
