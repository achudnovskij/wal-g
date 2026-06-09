package postgres

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMaxRetainedBytes(t *testing.T) {
	t.Setenv(MaxRetainedBytesEnv, "")
	if got := maxRetainedBytes(); got != defaultMaxRetainedBytes {
		t.Fatalf("unset: got %d, want default %d", got, defaultMaxRetainedBytes)
	}
	t.Setenv(MaxRetainedBytesEnv, "12345")
	if got := maxRetainedBytes(); got != 12345 {
		t.Fatalf("explicit: got %d, want 12345", got)
	}
	t.Setenv(MaxRetainedBytesEnv, "0") // disable
	if got := maxRetainedBytes(); got != 0 {
		t.Fatalf("zero (disabled): got %d, want 0", got)
	}
	t.Setenv(MaxRetainedBytesEnv, "garbage")
	if got := maxRetainedBytes(); got != defaultMaxRetainedBytes {
		t.Fatalf("invalid: got %d, want default %d", got, defaultMaxRetainedBytes)
	}
}

func TestRetainedBytes(t *testing.T) {
	dir := t.TempDir()
	if got := retainedBytes(dir); got != 0 {
		t.Fatalf("empty dir: got %d, want 0", got)
	}
	// two retained segments of 1000 and 2000 bytes
	if err := os.WriteFile(filepath.Join(dir, "seg1"), make([]byte, 1000), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "seg2.partial"), make([]byte, 2000), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := retainedBytes(dir); got != 3000 {
		t.Fatalf("got %d, want 3000 (subdirs excluded)", got)
	}
	// nonexistent dir -> 0, no panic
	if got := retainedBytes(filepath.Join(dir, "nope")); got != 0 {
		t.Fatalf("missing dir: got %d, want 0", got)
	}
}
