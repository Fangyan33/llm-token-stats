package events

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFixturesGolden(t *testing.T) {
	files, _ := filepath.Glob("../../testdata/collection/*.json")
	if len(files) != 6 {
		t.Fatal(len(files))
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			b, _ := os.ReadFile(f)
			idStart := []byte(`"logical_request_id": "`)
			idx := bytes.Index(b, idStart)
			id := b[idx+len(idStart) : idx+len(idStart)+36]
			e, err := Decode(b, id, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := Canonical(e)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := os.ReadFile(strings.TrimSuffix(f, ".json") + ".canonical")
			if !bytes.Equal(want, canonical) {
				t.Fatal("canonical mismatch")
			}
			h, _ := os.ReadFile(strings.TrimSuffix(f, ".json") + ".hashes")
			if string(h) != Hash(canonical)+"\n"+IdentityHash(e)+"\n" {
				t.Fatal("hash mismatch")
			}
			for _, bad := range [][]byte{bytes.Replace(b, []byte(`"schema_version": 1`), []byte(`"schema_version": 2`), 1), bytes.Replace(b, []byte(`"schema_version": 1`), []byte(`"schema_version": 1, "schema_version": 1`), 1)} {
				if _, err = Decode(bad, id, time.Time{}); err == nil {
					t.Fatal("accepted invalid event")
				}
			}
		})
	}
}

func TestRawUsageRejectsNull(t *testing.T) {
	for _, s := range []string{`{"prompt_tokens":null}`, `{"completion_tokens_details":{"reasoning_tokens":null}}`} {
		if RawValid(s) == nil {
			t.Fatal("raw usage null accepted as number")
		}
	}
}
