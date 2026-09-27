package eventbus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// dlq 是死信队列：超过重试上限的消息在此留存，绝不静默丢弃。
// 记录同时保存在内存与 dlq.jsonl（每行一条 JSON），进程重启后可重新加载。
type dlq struct {
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	records []DLQRecord
}

func newDLQ(dir string) (*dlq, error) {
	path := filepath.Join(dir, "dlq.jsonl")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open dlq: %w", err)
	}
	q := &dlq{file: f, writer: bufio.NewWriter(f)}
	if err := q.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return q, nil
}

// load 重启后把磁盘上的死信记录加载回内存。
func (q *dlq) load() error {
	if _, err := q.file.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(q.file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var r DLQRecord
		if err := json.Unmarshal(line, &r); err != nil {
			return fmt.Errorf("corrupt dlq record: %w", err)
		}
		q.records = append(q.records, r)
	}
	if _, err := q.file.Seek(0, 2); err != nil {
		return err
	}
	q.writer = bufio.NewWriter(q.file)
	return scanner.Err()
}

// add 追加一条死信记录并 fsync 持久化。
func (q *dlq) add(r DLQRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, err := q.writer.Write(data); err != nil {
		return err
	}
	if err := q.writer.WriteByte('\n'); err != nil {
		return err
	}
	if err := q.writer.Flush(); err != nil {
		return err
	}
	if err := q.file.Sync(); err != nil {
		return err
	}
	q.records = append(q.records, r)
	return nil
}

// snapshot 返回死信记录的副本，避免外部并发修改。
func (q *dlq) snapshot() []DLQRecord {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]DLQRecord, len(q.records))
	copy(out, q.records)
	return out
}

func (q *dlq) close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.writer.Flush(); err != nil {
		return err
	}
	return q.file.Close()
}
