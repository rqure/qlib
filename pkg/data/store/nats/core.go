package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	natsgo "github.com/nats-io/nats.go" // Changed import name to avoid conflict
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Config struct {
	Address   string
	ServiceId string // Added for queue group identification
}

type Core interface {
	Connect(ctx context.Context)
	Disconnect(ctx context.Context)
	IsConnected(ctx context.Context) bool
	Publish(subject string, msg proto.Message) error
	Request(ctx context.Context, subject string, msg proto.Message) (*protobufs.WebMessage, error)
	Subscribe(subject string, handler func(msg *natsgo.Msg))
	SetConfig(config Config)
	GetConfig() Config
	GetKeyGenerator() KeyGenerator
	QueueSubscribe(subject, queue string, handler func(msg *natsgo.Msg))
}

type coreInternal struct {
	config Config
	conn   *natsgo.Conn
	subs   []*natsgo.Subscription
	kg     KeyGenerator
	mu     sync.RWMutex
}

func NewCore(config Config) Core {
	return &coreInternal{
		config: config,
		kg:     NewKeyGenerator(),
	}
}

func (c *coreInternal) Connect(ctx context.Context) {
	c.Disconnect(ctx)

	opts := []natsgo.Option{
		natsgo.Timeout(10 * time.Second),
	}

	nc, err := natsgo.Connect(c.config.Address, opts...)
	if err != nil {
		log.Error("Failed to connect to NATS: %v", err)
		return
	}

	c.mu.Lock()
	c.conn = nc
	c.mu.Unlock()
}

func (c *coreInternal) Disconnect(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sub := range c.subs {
		sub.Unsubscribe()
	}
	c.subs = nil

	if c.conn != nil {
		c.conn.Drain() // Drain allows in-flight messages to complete
		c.conn.Close()
		c.conn = nil
	}
}

func (c *coreInternal) IsConnected(ctx context.Context) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && c.conn.IsConnected()
}

func (c *coreInternal) Publish(subject string, msg proto.Message) error {
	webMsg := &protobufs.WebMessage{}
	webMsg.Header = &protobufs.WebHeader{}
	webMsg.Payload, _ = anypb.New(msg)

	data, err := proto.Marshal(webMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	return c.conn.Publish(subject, data)
}

func (c *coreInternal) Request(ctx context.Context, subject string, msg proto.Message) (*protobufs.WebMessage, error) {
	webMsg := &protobufs.WebMessage{}
	webMsg.Header = &protobufs.WebHeader{}
	webMsg.Payload, _ = anypb.New(msg)

	data, err := proto.Marshal(webMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	response, err := c.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}

	var respMsg protobufs.WebMessage
	if err := proto.Unmarshal(response.Data, &respMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return &respMsg, nil
}

func (c *coreInternal) Subscribe(subject string, handler func(msg *natsgo.Msg)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		log.Error("Not connected")
		return
	}

	sub, err := c.conn.Subscribe(subject, handler)
	if err != nil {
		log.Error("Failed to subscribe: %v", err)
		return
	}

	c.subs = append(c.subs, sub)
}

func (c *coreInternal) QueueSubscribe(subject, queue string, handler func(msg *natsgo.Msg)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		log.Error("Not connected")
		return
	}

	sub, err := c.conn.QueueSubscribe(subject, queue, handler)
	if err != nil {
		log.Error("Failed to queue subscribe: %v", err)
		return
	}

	c.subs = append(c.subs, sub)
}

func (c *coreInternal) SetConfig(config Config) {
	c.config = config
}

func (c *coreInternal) GetConfig() Config {
	return c.config
}

func (c *coreInternal) GetKeyGenerator() KeyGenerator {
	return c.kg
}
