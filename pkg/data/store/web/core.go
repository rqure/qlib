package web

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	Address string
}

type responseCtxPair struct {
	ctx context.Context
	ch  chan *protobufs.ApiMessage
}

type Core interface {
	Connect(ctx context.Context)
	Disconnect(ctx context.Context)
	IsConnected(ctx context.Context) bool
	SendAndWait(ctx context.Context, msg web.Message) web.Message
	SetConfig(config Config)
	GetConfig() Config
}

type coreInternal struct {
	config           Config
	client           web.Client
	pendingResponses map[string]responseCtxPair
	mu               sync.RWMutex
}

func NewCore(config Config) Core {
	return &coreInternal{
		config:           config,
		pendingResponses: map[string]responseCtxPair{},
	}
}

func (c *coreInternal) Connect(ctx context.Context) {
	c.Disconnect(ctx)

	log.Info("Connecting to %v", c.config.Address)

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(c.config.Address, nil)
	if err != nil {
		log.Error("Failed to connect: %v", err)
		return
	}

	c.client = web.NewClient(conn, func(id string) {
		log.Info("Connection closed: %v", id)
		c.Disconnect(ctx)
	})

	c.client.SetMessageHandler(func(_ web.Client, msg web.Message) {
		c.handleMessage(msg)
	})
}

func (c *coreInternal) Disconnect(ctx context.Context) {
	if c.client != nil {
		c.client.Close()
		c.client = nil
	}
}

func (c *coreInternal) IsConnected(ctx context.Context) bool {
	if c.client == nil {
		return false
	}

	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiRuntimeGetDatabaseConnectionStatusRequest{})

	response := c.SendAndWait(ctx, msg)
	if response == nil {
		return false
	}

	var resp protobufs.ApiRuntimeGetDatabaseConnectionStatusResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Connected
}

func (c *coreInternal) SendAndWait(ctx context.Context, msg web.Message) web.Message {
	if c.client == nil {
		log.Error("Not connected")
		return nil
	}

	requestId := uuid.New().String()
	msg.Header.Id = requestId
	msg.Header.Timestamp = timestamppb.Now()

	responseCh := make(chan web.Message, 1)

	c.mu.Lock()
	c.pendingResponses[requestId] = responseCtxPair{
		ctx: ctx,
		ch:  responseCh,
	}
	c.mu.Unlock()

	c.client.Write(msg)

	select {
	case <-ctx.Done():
		log.Warn("Context done")
		return nil
	case response := <-responseCh:
		return response
	case <-time.After(10 * time.Second):
		log.Error("Timeout waiting for response to %v", requestId)
		c.mu.Lock()
		delete(c.pendingResponses, requestId)
		c.mu.Unlock()
		return nil
	}
}

func (c *coreInternal) handleMessage(msg web.Message) {
	c.mu.Lock()
	if rsp, ok := c.pendingResponses[msg.Header.Id]; ok {
		delete(c.pendingResponses, msg.Header.Id)
		c.mu.Unlock()
		select {
		case <-rsp.ctx.Done():
			log.Info("Context done")
		case rsp.ch <- msg:
			log.Trace("Returned response for headerId=(%s)", msg.Header.Id)
		}
		close(rsp.ch)
		return
	}
	c.mu.Unlock()
}

func (c *coreInternal) SetConfig(config Config) {
	c.config = config
}

func (c *coreInternal) GetConfig() Config {
	return c.config
}
