package eventbus

import "time"

// RetryPolicy 描述失败重试的退避规则。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次），必须 >=1。
	MaxAttempts int
	// InitialBackoff 首次失败后的退避时长。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限，为 0 时取 InitialBackoff。
	MaxBackoff time.Duration
	// Multiplier 退避乘数，<1 时按 1 处理（固定退避）。
	Multiplier float64
}

// Config 是总线配置。
type Config struct {
	// DataDir 本地持久化目录（分区日志与位点文件），不存在会自动创建。
	DataDir string
	// Partitions 分区数量，必须 >=1。
	Partitions int
	// PartitionQueueCapacity 每个消费组、每个分区的在途消息队列容量，用于背压。
	PartitionQueueCapacity int
	// BlockWhenFull 为 true 时 Produce 在队列满时阻塞等待（受 ctx 控制）；
	// 为 false 时立即返回 ErrQueueFull。
	BlockWhenFull bool
	// Retry 失败重试策略。
	Retry RetryPolicy
}

// DefaultConfig 返回一份可直接本地运行的默认配置。
func DefaultConfig() Config {
	return Config{
		DataDir:                "eventbus-data",
		Partitions:             2,
		PartitionQueueCapacity: 128,
		BlockWhenFull:          false,
		Retry: RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
			Multiplier:     2,
		},
	}
}

func (c *Config) normalize() {
	if c.Partitions < 1 {
		c.Partitions = 1
	}
	if c.PartitionQueueCapacity < 1 {
		c.PartitionQueueCapacity = 1
	}
	if c.Retry.MaxAttempts < 1 {
		c.Retry.MaxAttempts = 1
	}
	if c.Retry.InitialBackoff < 0 {
		c.Retry.InitialBackoff = 0
	}
	if c.Retry.MaxBackoff < c.Retry.InitialBackoff {
		c.Retry.MaxBackoff = c.Retry.InitialBackoff
	}
	if c.Retry.Multiplier < 1 {
		c.Retry.Multiplier = 1
	}
}

// backoff 返回第 attempt 次失败（attempt 从 1 开始）后应等待的时长。
func (p RetryPolicy) backoff(attempt int) time.Duration {
	d := p.InitialBackoff
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * p.Multiplier)
		if d >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	if d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	return d
}
