package qnats

import (
	"context"
	"fmt"
	"sync"

	natsgo "github.com/nats-io/nats.go" // Changed import name to avoid conflict
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type NatsConfig struct {
	Address string
}

type NatsCore interface {
	Connect(ctx context.Context)
	Disconnect(ctx context.Context)
	IsConnected(ctx context.Context) bool
	Publish(subject string, msg proto.Message) error
	Request(ctx context.Context, subject string, msg proto.Message) (*qprotobufs.ApiMessage, error)
	Subscribe(subject string, handler natsgo.MsgHandler)
	SetConfig(config NatsConfig)
	GetConfig() NatsConfig
	GetKeyGenerator() NatsKeyGenerator
	QueueSubscribe(subject string, handler natsgo.MsgHandler)

	SetAuthProvider(qdata.AuthProvider)

	Connected() qss.Signal[qss.VoidType]
	Disconnected() qss.Signal[error]
	BeforeConnected() qss.Signal[qss.VoidType]
}

type natsCore struct {
	config NatsConfig
	conn   *natsgo.Conn
	subs   []*natsgo.Subscription
	kg     NatsKeyGenerator
	mu     sync.RWMutex

	ap qdata.AuthProvider

	beforeConnected qss.Signal[qss.VoidType]
	connected       qss.Signal[qss.VoidType]
	disconnected    qss.Signal[error]
}

func NewCore(config NatsConfig) NatsCore {
	return &natsCore{
		config:          config,
		kg:              NewKeyGenerator(),
		connected:       qss.New[qss.VoidType](),
		disconnected:    qss.New[error](),
		beforeConnected: qss.New[qss.VoidType](),
	}
}

func (me *natsCore) BeforeConnected() qss.Signal[qss.VoidType] {
	return me.beforeConnected
}

func (c *natsCore) SetAuthProvider(sp qdata.AuthProvider) {
	c.ap = sp
}

func (c *natsCore) Connect(ctx context.Context) {
	c.Disconnect(ctx)

	opts := []natsgo.Option{
		natsgo.MaxReconnects(-1),
		natsgo.ConnectHandler(func(nc *natsgo.Conn) {
			c.beforeConnected.Emit(qss.Void)
			c.connected.Emit(qss.Void)
		}),
		natsgo.ReconnectHandler(func(nc *natsgo.Conn) {
			c.cleanupSubscriptions()
			c.beforeConnected.Emit(qss.Void)
			c.connected.Emit(qss.Void)
		}),
		natsgo.DisconnectErrHandler(func(nc *natsgo.Conn, err error) {
			c.disconnected.Emit(err)
		}),
	}

	nc, err := natsgo.Connect(c.config.Address, opts...)
	if err != nil {
		qlog.Error("Failed to connect to NATS: %v", err)
		return
	}

	c.mu.Lock()
	c.conn = nc
	c.mu.Unlock()
}

func (c *natsCore) Disconnect(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Drain() // Drain allows in-flight messages to complete
		c.conn.Close()
		c.conn = nil
	}
}

func (c *natsCore) IsConnected(ctx context.Context) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && c.conn.IsConnected()
}

func (c *natsCore) Publish(subject string, msg proto.Message) error {
	apiMsg := &qprotobufs.ApiMessage{}
	apiMsg.Header = &qprotobufs.ApiHeader{}
	apiMsg.Payload, _ = anypb.New(msg)

	data, err := proto.Marshal(apiMsg)
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

func (c *natsCore) Request(ctx context.Context, subject string, msg proto.Message) (*qprotobufs.ApiMessage, error) {
	apiMsg := &qprotobufs.ApiMessage{}
	apiMsg.Header = &qprotobufs.ApiHeader{}
	apiMsg.Payload, _ = anypb.New(msg)

	if c.ap != nil {
		client := c.ap.AuthClient(ctx)
		if client != nil {
			session := client.GetSession(ctx)
			apiMsg.Header.AccessToken = session.AccessToken()
		}
	}

	data, err := proto.Marshal(apiMsg)
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

	var respMsg qprotobufs.ApiMessage
	if err := proto.Unmarshal(response.Data, &respMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return &respMsg, nil
}

func (c *natsCore) Subscribe(subject string, handler natsgo.MsgHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		qlog.Error("Not connected")
		return
	}

	sub, err := c.conn.Subscribe(subject, handler)
	if err != nil {
		qlog.Error("Failed to subscribe: %v", err)
		return
	}

	c.subs = append(c.subs, sub)
}

func (c *natsCore) QueueSubscribe(subject string, handler natsgo.MsgHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		qlog.Error("Not connected")
		return
	}

	sub, err := c.conn.QueueSubscribe(subject, qapp.GetName(), handler)
	if err != nil {
		qlog.Error("Failed to queue subscribe: %v", err)
		return
	}

	c.subs = append(c.subs, sub)
}

func (c *natsCore) SetConfig(config NatsConfig) {
	c.config = config
}

func (c *natsCore) GetConfig() NatsConfig {
	return c.config
}

func (c *natsCore) GetKeyGenerator() NatsKeyGenerator {
	return c.kg
}

func (c *natsCore) Connected() qss.Signal[qss.VoidType] {
	return c.connected
}

func (c *natsCore) Disconnected() qss.Signal[error] {
	return c.disconnected
}

func (c *natsCore) cleanupSubscriptions() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sub := range c.subs {
		sub.Unsubscribe()
	}
	c.subs = nil
}
