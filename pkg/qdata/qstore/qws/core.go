package qws

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	// DefaultDialTimeout is the default timeout for dialing a WebSocket connection
	DefaultDialTimeout = 10 * time.Second

	// DefaultReconnectInterval is the default interval between reconnection attempts
	DefaultReconnectInterval = 5 * time.Second

	// DefaultPingInterval is the default interval between ping messages
	DefaultPingInterval = 30 * time.Second
)

// WebSocketConfig holds the configuration for a WebSocket connection
type WebSocketConfig struct {
	URL          string
	DialTimeout  time.Duration
	PingInterval time.Duration
}

// WebSocketCore defines the interface for WebSocket-based communication
type WebSocketCore interface {
	Connect(ctx context.Context)
	Disconnect(ctx context.Context)
	IsConnected() bool

	Connected() qss.Signal[qdata.ConnectedArgs]
	Disconnected() qss.Signal[qdata.DisconnectedArgs]
	EventMsg() qss.Signal[*qprotobufs.ApiMessage]

	Publish(ctx context.Context, msg proto.Message) error
	Request(ctx context.Context, msg proto.Message) (*qprotobufs.ApiMessage, error)

	SetConfig(config WebSocketConfig)
	GetConfig() WebSocketConfig
}

type webSocketCore struct {
	config      WebSocketConfig
	conn        *websocket.Conn
	isConnected bool
	lock        sync.RWMutex
	pendingReqs map[string]chan *qprotobufs.ApiMessage
	reqLock     sync.RWMutex
	done        chan struct{}
	reconnectCh chan struct{}

	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]
	eventMsg     qss.Signal[*qprotobufs.ApiMessage]
}

// NewCore creates a new WebSocketCore instance
func NewCore(config WebSocketConfig) WebSocketCore {
	// Set default values if not provided
	if config.DialTimeout == 0 {
		config.DialTimeout = DefaultDialTimeout
	}
	if config.PingInterval == 0 {
		config.PingInterval = DefaultPingInterval
	}

	return &webSocketCore{
		config:       config,
		pendingReqs:  make(map[string]chan *qprotobufs.ApiMessage),
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
		eventMsg:     qss.New[*qprotobufs.ApiMessage](),
		reconnectCh:  make(chan struct{}),
	}
}

func (me *webSocketCore) setConnected(ctx context.Context, connected bool, err error) {
	if me.isConnected == connected {
		return
	}

	me.isConnected = connected
	if connected {
		handle := qcontext.GetHandle(ctx)
		handle.DoInMainThread(func(ctx context.Context) {
			me.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})
		})
	} else {
		handle := qcontext.GetHandle(ctx)
		handle.DoInMainThread(func(ctx context.Context) {
			me.disconnected.Emit(qdata.DisconnectedArgs{
				Ctx: ctx,
				Err: err,
			})
		})
	}
}

func (me *webSocketCore) Connect(ctx context.Context) {
	me.lock.Lock()
	if me.isConnected {
		me.lock.Unlock()
		return
	}

	// Close any existing connection
	me.cleanupConnection()
	me.done = make(chan struct{})
	me.lock.Unlock()

	go me.connectLoop(ctx)
}

func (me *webSocketCore) connectLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := me.doConnect(ctx); err != nil {
				qlog.Warn("WebSocket connection failed: %v, retrying in %s", err, DefaultReconnectInterval)
				time.Sleep(DefaultReconnectInterval)
				continue
			}

			// Start read loop
			go me.readLoop(ctx)

			// Start ping loop
			go me.pingLoop(ctx)

			// Wait for reconnect signal or context cancel
			select {
			case <-ctx.Done():
				return
			case <-me.reconnectCh:
				qlog.Info("Reconnecting WebSocket...")
				continue
			}
		}
	}
}

func (me *webSocketCore) doConnect(ctx context.Context) error {
	me.lock.Lock()
	defer me.lock.Unlock()

	// Parse the URL
	u, err := url.Parse(me.config.URL)
	if err != nil {
		return fmt.Errorf("invalid WebSocket URL: %w", err)
	}

	// Dial with timeout
	dialCtx, cancel := context.WithTimeout(ctx, me.config.DialTimeout)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("WebSocket dial failed: %w", err)
	}

	me.conn = conn
	me.setConnected(ctx, true, nil)

	return nil
}

func (me *webSocketCore) readLoop(ctx context.Context) {
	defer func() {
		me.signalReconnect(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-me.done:
			return
		default:
			if !me.IsConnected() {
				return
			}

			me.lock.RLock()
			conn := me.conn
			me.lock.RUnlock()

			if conn == nil {
				return
			}

			_, data, err := conn.Read(ctx)
			if err != nil {
				qlog.Warn("WebSocket read error: %v", err)
				return
			}

			var apiMsg qprotobufs.ApiMessage
			if err := proto.Unmarshal(data, &apiMsg); err != nil {
				qlog.Warn("Failed to unmarshal WebSocket message: %v", err)
				continue
			}

			// Process the message
			me.processMessage(&apiMsg)
		}
	}
}

func (me *webSocketCore) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(me.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-me.done:
			return
		case <-ticker.C:
			if !me.IsConnected() {
				return
			}

			me.lock.RLock()
			conn := me.conn
			me.lock.RUnlock()

			if conn == nil {
				return
			}

			if err := conn.Ping(ctx); err != nil {
				qlog.Error("WebSocket ping failed: %v", err)
				me.signalReconnect(ctx)
				return
			}
		}
	}
}

func (me *webSocketCore) processMessage(msg *qprotobufs.ApiMessage) {
	if msg == nil || msg.Header == nil {
		return
	}

	msgID := msg.Header.Id

	// Check if this is a response to a request
	me.reqLock.RLock()
	respCh, exists := me.pendingReqs[msgID]
	me.reqLock.RUnlock()

	if exists {
		// Send response to the waiting goroutine
		respCh <- msg
	} else {
		me.eventMsg.Emit(msg)
	}
}

func (me *webSocketCore) Disconnect(ctx context.Context) {
	me.lock.Lock()
	defer me.lock.Unlock()

	me.cleanupConnection()
	me.setConnected(ctx, false, nil)
}

func (me *webSocketCore) cleanupConnection() {
	if me.conn != nil {
		me.conn.Close(websocket.StatusNormalClosure, "Client disconnecting")
		me.conn = nil
	}

	if me.done != nil {
		close(me.done)
		me.done = nil
	}

	// Close all pending request channels
	me.reqLock.Lock()
	for id, ch := range me.pendingReqs {
		close(ch)
		delete(me.pendingReqs, id)
	}
	me.reqLock.Unlock()
}

func (me *webSocketCore) IsConnected() bool {
	me.lock.RLock()
	defer me.lock.RUnlock()
	return me.isConnected
}

func (me *webSocketCore) signalReconnect(ctx context.Context) {
	me.lock.Lock()
	defer me.lock.Unlock()

	if me.isConnected {
		me.setConnected(ctx, false, fmt.Errorf("connection lost"))

		// Signal reconnection
		select {
		case me.reconnectCh <- struct{}{}:
		default:
			// Channel is already signaled
		}
	}
}

func (me *webSocketCore) Publish(ctx context.Context, msg proto.Message) error {
	apiMsg := &qprotobufs.ApiMessage{
		Header:  &qprotobufs.ApiHeader{Id: uuid.New().String()},
		Payload: nil,
	}

	var err error
	apiMsg.Payload, err = anypb.New(msg)
	if err != nil {
		return fmt.Errorf("failed to pack message: %w", err)
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	me.lock.RLock()
	conn := me.conn
	isConnected := me.isConnected
	me.lock.RUnlock()

	if !isConnected || conn == nil {
		return errors.New("not connected")
	}

	// Use the provided context for this write operation
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		qlog.Error("WebSocket write error: %v", err)
		me.signalReconnect(ctx)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (me *webSocketCore) Request(ctx context.Context, msg proto.Message) (*qprotobufs.ApiMessage, error) {
	// Create message ID for correlation
	msgID := uuid.New().String()

	apiMsg := &qprotobufs.ApiMessage{
		Header: &qprotobufs.ApiHeader{Id: msgID},
	}

	// Add authentication if available
	clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
	client := clientProvider.Client(ctx)
	if client != nil {
		session := client.GetSession(ctx)
		apiMsg.Header.AccessToken = session.AccessToken()
	}

	var err error
	apiMsg.Payload, err = anypb.New(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to pack message: %w", err)
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	// Register the response channel before sending
	respCh := make(chan *qprotobufs.ApiMessage, 1)
	me.reqLock.Lock()
	me.pendingReqs[msgID] = respCh
	me.reqLock.Unlock()

	// Clean up when done
	defer func() {
		me.reqLock.Lock()
		delete(me.pendingReqs, msgID)
		me.reqLock.Unlock()
	}()

	me.lock.RLock()
	conn := me.conn
	isConnected := me.isConnected
	me.lock.RUnlock()

	if !isConnected || conn == nil {
		return nil, errors.New("not connected")
	}

	// Use the provided context for the write operation
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		me.signalReconnect(ctx)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Wait for response with timeout controlled by the provided context
	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (me *webSocketCore) SetConfig(config WebSocketConfig) {
	me.config = config
}

func (me *webSocketCore) GetConfig() WebSocketConfig {
	return me.config
}

func (me *webSocketCore) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

func (me *webSocketCore) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}

func (me *webSocketCore) EventMsg() qss.Signal[*qprotobufs.ApiMessage] {
	return me.eventMsg
}
