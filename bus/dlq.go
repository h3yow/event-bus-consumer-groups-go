package bus

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// DLQ records dead-lettered messages durably (one JSON object per line,
// fsynced on append) and serves them for inspection.
type DLQ struct {
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	path string
}

// OpenDLQ opens (or creates) the dead-letter file at path.
func OpenDLQ(path string) (*DLQ, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &DLQ{f: f, w: bufio.NewWriter(f), path: path}, nil
}

// Append durably records a dead-letter entry.
func (d *DLQ) Append(entry DLQEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := d.w.Write(line); err != nil {
		return err
	}
	if err := d.w.Flush(); err != nil {
		return err
	}
	return d.f.Sync()
}

// Entries returns all dead-letter entries currently persisted.
func (d *DLQ) Entries() ([]DLQEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Ensure buffered writes are visible to the reader, but do not rely on
	// the buffered writer for reads: re-open read-only.
	if err := d.w.Flush(); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(d.path)
	if err != nil {
		return nil, err
	}
	var out []DLQEntry
	start := 0
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\n' {
			continue
		}
		line := raw[start:i]
		start = i + 1
		if len(line) == 0 {
			continue
		}
		var e DLQEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// Close flushes and closes the DLQ file.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.w.Flush(); err != nil {
		_ = d.f.Close()
		return err
	}
	return d.f.Close()
}
