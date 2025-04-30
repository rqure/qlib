package qnats

import (
	"context"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go" // Changed import name to avoid conflict
	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qcontext"
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
	IsConnected() bool
	CheckConnection(ctx context.Context) bool

	BeforeConnected() qss.Signal[qdata.ConnectedArgs]
	Connected() qss.Signal[qdata.ConnectedArgs]
	Disconnected() qss.Signal[qdata.DisconnectedArgs]

	Publish(subject string, msg proto.Message) error
	Request(ctx context.Context, subject string, msg proto.Message) (*qprotobufs.ApiMessage, error)

	QueueSubscribe(subject, queue string, handler natsgo.MsgHandler)
	Subscribe(subject string, handler natsgo.MsgHandler)

	SetConfig(config NatsConfig)
	GetConfig() NatsConfig
	GetKeyGenerator() NatsKeyGenerator
}

type natsCore struct {
	config NatsConfig
	conn   *natsgo.Conn
	subs   []*natsgo.Subscription
	kg     NatsKeyGenerator

	isConnected bool

	beforeConnected qss.Signal[qdata.ConnectedArgs]
	connected       qss.Signal[qdata.ConnectedArgs]
	disconnected    qss.Signal[qdata.DisconnectedArgs]
}

func NewCore(config NatsConfig) NatsCore {
	return &natsCore{
		config:          config,
		kg:              NewKeyGenerator(),
		connected:       qss.New[qdata.ConnectedArgs](),
		disconnected:    qss.New[qdata.DisconnectedArgs](),
		beforeConnected: qss.New[qdata.ConnectedArgs](),
	}
}

func (me *natsCore) BeforeConnected() qss.Signal[qdata.ConnectedArgs] {
	return me.beforeConnected
}

func (me *natsCore) Connect(ctx context.Context) {
	me.Disconnect(ctx)

	opts := []natsgo.Option{
		natsgo.MaxReconnects(0),
		natsgo.ConnectHandler(func(nc *natsgo.Conn) {
			me.onConnected(ctx)
		}),
		natsgo.ReconnectHandler(func(nc *natsgo.Conn) {
			me.onConnected(ctx)
		}),
		natsgo.DisconnectErrHandler(func(nc *natsgo.Conn, err error) {
			me.onDisconnected(ctx, err)
		}),
	}

	nc, err := natsgo.Connect(me.config.Address, opts...)
	if err != nil {
		qlog.Error("Failed to connect to NATS: %v", err)
		return
	}

	me.conn = nc
}

func (me *natsCore) Disconnect(ctx context.Context) {
	if me.conn != nil {
		me.cleanupSubscriptions()
		me.conn.Drain() // Drain allows in-flight messages to complete
		me.conn.Close()
		me.conn = nil
	}

	me.isConnected = false
}

func (me *natsCore) IsConnected() bool {
	return me.isConnected
}

func (me *natsCore) CheckConnection(ctx context.Context) bool {
	return me.IsConnected()
}

func (me *natsCore) Publish(subject string, msg proto.Message) error {
	apiMsg := &qprotobufs.ApiMessage{}
	apiMsg.Header = &qprotobufs.ApiHeader{}
	apiMsg.Payload, _ = anypb.New(msg)

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	if me.conn == nil {
		return fmt.Errorf("not connected")
	}

	return me.conn.Publish(subject, data)
}

func (c *natsCore) Request(ctx context.Context, subject string, msg proto.Message) (*qprotobufs.ApiMessage, error) {
	apiMsg := &qprotobufs.ApiMessage{}
	apiMsg.Header = &qprotobufs.ApiHeader{}
	apiMsg.Payload, _ = anypb.New(msg)

	startTime := time.Now()
	clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
	client := clientProvider.Client(ctx)
	if client != nil {
		session := client.GetSession(ctx)
		apiMsg.Header.AccessToken = session.AccessToken()
		qlog.Trace("Retrieving access token took %s", time.Since(startTime))
	}

	startTime = time.Now()
	data, err := proto.Marshal(apiMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}
	qlog.Trace("Marshalling message took %s", time.Since(startTime))

	if c.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	startTime = time.Now()
	response, err := c.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	qlog.Trace("Request to %s took %s", subject, time.Since(startTime))

	startTime = time.Now()
	var respMsg qprotobufs.ApiMessage
	if err := proto.Unmarshal(response.Data, &respMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}
	qlog.Trace("Unmarshalling response took %s", time.Since(startTime))

	return &respMsg, nil
}

func (c *natsCore) Subscribe(subject string, handler natsgo.MsgHandler) {
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

func (c *natsCore) QueueSubscribe(subject, queue string, handler natsgo.MsgHandler) {
	if c.conn == nil {
		qlog.Error("Not connected")
		return
	}

	sub, err := c.conn.QueueSubscribe(subject, queue, handler)
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

func (c *natsCore) Connected() qss.Signal[qdata.ConnectedArgs] {
	return c.connected
}

func (c *natsCore) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return c.disconnected
}

func (c *natsCore) cleanupSubscriptions() {
	for _, sub := range c.subs {
		sub.Unsubscribe()
	}
	c.subs = nil
}

func (c *natsCore) onConnected(ctx context.Context) {
	handle := qcontext.GetHandle(ctx)

	handle.DoInMainThread(func(ctx context.Context) {
		if c.isConnected {
			return
		}

		c.isConnected = true

		c.beforeConnected.Emit(qdata.ConnectedArgs{
			Ctx: ctx,
		})

		c.connected.Emit(qdata.ConnectedArgs{
			Ctx: ctx,
		})
	})

}

func (c *natsCore) onDisconnected(ctx context.Context, err error) {
	handle := qcontext.GetHandle(ctx)

	handle.DoInMainThread(func(ctx context.Context) {
		if !c.isConnected {
			return
		}

		c.Disconnect(ctx)

		c.disconnected.Emit(qdata.DisconnectedArgs{
			Ctx: ctx,
			Err: err,
		})
	})
}
