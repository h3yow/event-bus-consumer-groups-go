package eventbus_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	eventbus "github.com/h3yow/event-bus-consumer-groups-go"
)

func ExampleBus() {
	tmp, _ := os.MkdirTemp("", "eventbus-example")
	defer os.RemoveAll(tmp)
	dir := filepath.Join(tmp, "example-data")
	cfg := eventbus.DefaultConfig()
	cfg.DataDir = dir
	cfg.Partitions = 1
	cfg.BlockWhenFull = true
	cfg.Retry.MaxAttempts = 2

	bus, err := eventbus.NewBus(cfg)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	if err := bus.Subscribe("demo", func(ctx context.Context, m eventbus.Message) error {
		defer wg.Done()
		fmt.Printf("consumed %s attempt=%d\n", m.ID, m.Attempt)
		return nil
	}); err != nil {
		panic(err)
	}

	stored, err := bus.Produce(context.Background(), eventbus.Message{ID: "m1", Key: "user-1"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("produced %s partition=%d offset=%d\n", stored.ID, stored.Partition, stored.Offset)
	wg.Wait()
	_ = bus.Close()

	// 可区分的拒绝原因可通过 errors.Is 判断：
	_ = errors.Is(err, eventbus.ErrQueueFull)
	// Output:
	// produced m1 partition=0 offset=0
	// consumed m1 attempt=1
}
