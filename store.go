package eventbus

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-' || c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// storedMessage 是分区日志中每条记录的磁盘表示。
type storedMessage struct {
	ID      string `json:"id"`
	Key     string `json:"key"`
	Payload []byte `json:"payload"`
}

// partitionLog 是单个分区的持久化仅追加日志。
// 记录帧格式：4 字节大端长度 + JSON 负载。位点等于记录的追加序号（从 0 起）。
type partitionLog struct {
	mu    sync.Mutex // 保护共享文件句柄的偏移与 index
	file  *os.File
	index []int64 // index[off] = 该位点记录在文件中的字节偏移
}

func newPartitionLog(dir string, p Partition) (*partitionLog, error) {
	path := filepath.Join(dir, "partitions", fmt.Sprintf("partition-%d.log", p))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open partition %d log: %w", p, err)
	}
	l := &partitionLog{file: f}
	if err := l.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// replay 重建位点 -> 字节偏移索引，供重启后按位点读取。
func (l *partitionLog) replay() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var lenBuf [4]byte
	for {
		pos, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(l.file, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // 末尾：无完整帧
			}
			return err
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		if n > 1<<28 { // 256MiB 单条上限，防止损坏文件导致巨量分配
			return fmt.Errorf("corrupt frame length %d at %d", n, pos)
		}
		if _, err := l.file.Seek(int64(n), io.SeekCurrent); err != nil {
			return err
		}
		l.index = append(l.index, pos)
	}
	return nil
}

// append 追加一条消息并返回分配的位点；fsync 后才返回，保证崩溃可恢复。
func (l *partitionLog) append(m *Message) (Offset, error) {
	data, err := json.Marshal(storedMessage{ID: m.ID, Key: m.Key, Payload: m.Payload})
	if err != nil {
		return InvalidOffset, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// O_APPEND 下新记录起始位置即文件当前长度。
	pos, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return InvalidOffset, err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := l.file.Write(lenBuf[:]); err != nil {
		return InvalidOffset, err
	}
	if _, err := l.file.Write(data); err != nil {
		return InvalidOffset, err
	}
	if err := l.file.Sync(); err != nil {
		return InvalidOffset, err
	}
	l.index = append(l.index, pos)
	return Offset(len(l.index) - 1), nil
}

// readAt 读取指定位点的消息。
func (l *partitionLog) readAt(off Offset) (Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if off < 0 || int(off) >= len(l.index) {
		return Message{}, ErrOffsetOutOfRange
	}
	if _, err := l.file.Seek(l.index[off], io.SeekStart); err != nil {
		return Message{}, err
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(l.file, lenBuf[:]); err != nil {
		return Message{}, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(l.file, buf); err != nil {
		return Message{}, err
	}
	var sm storedMessage
	if err := json.Unmarshal(buf, &sm); err != nil {
		return Message{}, err
	}
	return Message{ID: sm.ID, Key: sm.Key, Payload: sm.Payload, Offset: off}, nil
}

// endOffset 返回下一条将被分配的位点。
func (l *partitionLog) endOffset() Offset {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Offset(len(l.index))
}

func (l *partitionLog) close() error { return l.file.Close() }

// partitionOffset 是一个（消费组, 分区）的位点管理：独立文件 + 独立锁，
// 因此不同分区/组的位点提交彼此并行，不共享全局锁，也不存在同路径 rename 竞争。
type partitionOffset struct {
	mu  sync.Mutex // 串行化本分区串行消费循环上的提交；同时用于恢复时的读改写
	dir string
	key string // 如 "group-g1/partition-0.offset"
}

// offsetStore 负责按（组, 分区）定位独立的 partitionOffset 与文件。
type offsetStore struct {
	dir   string
	mu    sync.RWMutex
	items map[string]*partitionOffset
	sync  bool // 是否对位点文件启用 fsync（默认开启；测试可调）
}

func newOffsetStore(dir string) (*offsetStore, error) {
	d := filepath.Join(dir, "offsets")
	if err := os.MkdirAll(d, 0o755); err != nil {
		return nil, err
	}
	return &offsetStore{dir: d, items: map[string]*partitionOffset{}, sync: true}, nil
}

// poKey 生成 (group, partition) 的内存映射键与相对文件路径。
func poKey(group string, p Partition) (mapKey, relPath string) {
	gdir := "group-" + sanitize(group)
	return gdir + "/p" + strconv.Itoa(int(p)), filepath.Join(gdir, fmt.Sprintf("partition-%d.offset", p))
}

func (s *offsetStore) po(group string, p Partition) *partitionOffset {
	key, rel := poKey(group, p)
	s.mu.RLock()
	item, ok := s.items[key]
	s.mu.RUnlock()
	if ok {
		return item
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if item, ok = s.items[key]; ok {
		return item
	}
	item = &partitionOffset{dir: filepath.Join(s.dir, filepath.Dir(rel)), key: key}
	s.items[key] = item
	return item
}

// commit 持久化位点。位点回退（新位点小于已磁盘/内存已确认值）被明确拒绝。
// 采用 唯一临时文件 + fsync + 原子 rename；每个（组,分区）的目标路径不同，
// 因此并发提交只受自身锁约束，互不阻塞。
func (s *offsetStore) commit(group string, p Partition, off Offset) error {
	if off < 0 {
		return fmt.Errorf("%w: group=%s partition=%d submitted=%d",
			ErrOffsetRollback, group, p, off)
	}
	po := s.po(group, p)
	po.mu.Lock()
	defer po.mu.Unlock()
	cur, err := po.load()
	if err != nil {
		return err
	}
	if cur != InvalidOffset && off < cur {
		return fmt.Errorf("%w: group=%s partition=%d committed=%d submitted=%d",
			ErrOffsetRollback, group, p, cur, off)
	}
	return po.persist(off, s.sync)
}

// get 返回某（组,分区）的已提交位点；新组 / 新分区返回 InvalidOffset。
func (s *offsetStore) get(group string, p Partition) (Offset, error) {
	return s.po(group, p).load()
}

// load 从磁盘读取当前值（无文件视为 InvalidOffset）。调用方持有 po.mu。
func (po *partitionOffset) load() (Offset, error) {
	data, err := os.ReadFile(filepath.Join(po.dir, filepath.Base(po.key)))
	if err != nil {
		if os.IsNotExist(err) {
			return InvalidOffset, nil
		}
		return InvalidOffset, err
	}
	var off int64
	if err := json.Unmarshal(bytes.TrimSpace(data), &off); err != nil {
		return InvalidOffset, fmt.Errorf("decode %s: %w", po.key, err)
	}
	return Offset(off), nil
}

// persist 写入新位点：tmp + fsync + 同目录原子替换。
func (po *partitionOffset) persist(off Offset, doSync bool) error {
	if err := os.MkdirAll(po.dir, 0o755); err != nil {
		return err
	}
	final := filepath.Join(po.dir, filepath.Base(po.key))
	tmp, err := os.CreateTemp(po.dir, ".w-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := fmt.Fprintf(tmp, "%d\n", int64(off)); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if doSync {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			cleanup()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return err
	}
	return nil
}
