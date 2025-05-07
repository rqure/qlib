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
	CheckConnection(ctx context.Context) bool

	BeforeConnected() qss.Signal[qdata.ConnectedArgs]
	Connected() qss.Signal[qdata.ConnectedArgs]
	Disconnected() qss.Signal[qdata.DisconnectedArgs]

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

	beforeConnected qss.Signal[qdata.ConnectedArgs]
	connected       qss.Signal[qdata.ConnectedArgs]
	disconnected    qss.Signal[qdata.DisconnectedArgs]
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
		config:          config,
		pendingReqs:     make(map[string]chan *qprotobufs.ApiMessage),
		beforeConnected: qss.New[qdata.ConnectedArgs](),
		connected:       qss.New[qdata.ConnectedArgs](),
		disconnected:    qss.New[qdata.DisconnectedArgs](),
		reconnectCh:     make(chan struct{}),
	}
}

func (wc *webSocketCore) BeforeConnected() qss.Signal[qdata.ConnectedArgs] {
	return wc.beforeConnected
}

func (wc *webSocketCore) Connect(ctx context.Context) {
	wc.lock.Lock()
	if wc.isConnected {
		wc.lock.Unlock()
		return
	}

	// Close any existing connection
	wc.cleanupConnection()
	wc.done = make(chan struct{})
	wc.lock.Unlock()

	go wc.connectLoop(ctx)
}

func (wc *webSocketCore) connectLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := wc.doConnect(ctx); err != nil {
				qlog.Error("WebSocket connection failed: %v, retrying in %s", err, DefaultReconnectInterval)
				time.Sleep(DefaultReconnectInterval)
				continue
			}

			// Start read loop
			go wc.readLoop(ctx)

			// Start ping loop
			go wc.pingLoop(ctx)

			// Wait for reconnect signal or context cancel
			select {
			case <-ctx.Done():
				return
			case <-wc.reconnectCh:
				qlog.Info("Reconnecting WebSocket...")
				continue
			}
		}
	}
}

func (wc *webSocketCore) doConnect(ctx context.Context) error {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	// Parse the URL
	u, err := url.Parse(wc.config.URL)
	if err != nil {
		return fmt.Errorf("invalid WebSocket URL: %w", err)
	}

	// Dial with timeout
	dialCtx, cancel := context.WithTimeout(ctx, wc.config.DialTimeout)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("WebSocket dial failed: %w", err)
	}

	wc.conn = conn
	wc.isConnected = true

	// Emit the connected signal
	handle := qcontext.GetHandle(ctx)
	handle.DoInMainThread(func(ctx context.Context) {
		wc.beforeConnected.Emit(qdata.ConnectedArgs{Ctx: ctx})
		wc.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})
	})

	return nil
}

func (wc *webSocketCore) readLoop(ctx context.Context) {
	defer func() {
		wc.signalReconnect()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wc.done:
			return
		default:
			if !wc.IsConnected() {
				return
			}

			wc.lock.RLock()
			conn := wc.conn
			wc.lock.RUnlock()

			if conn == nil {
				return
			}

			_, data, err := conn.Read(ctx)
			if err != nil {
				qlog.Error("WebSocket read error: %v", err)
				return
			}

			var apiMsg qprotobufs.ApiMessage
			if err := proto.Unmarshal(data, &apiMsg); err != nil {
				qlog.Error("Failed to unmarshal WebSocket message: %v", err)
				continue
			}

			// Process the message
			wc.processMessage(&apiMsg)
		}
	}
}

func (wc *webSocketCore) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(wc.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wc.done:
			return
		case <-ticker.C:
			if !wc.IsConnected() {
				return
			}

			wc.lock.RLock()
			conn := wc.conn
			wc.lock.RUnlock()

			if conn == nil {
				return
			}

			if err := conn.Ping(ctx); err != nil {
				qlog.Error("WebSocket ping failed: %v", err)
				wc.signalReconnect()
				return
			}
		}
	}
}

func (wc *webSocketCore) processMessage(msg *qprotobufs.ApiMessage) {
	if msg == nil || msg.Header == nil {
		return
	}

	msgID := msg.Header.Id

	// Check if this is a response to a request
	wc.reqLock.RLock()
	respCh, exists := wc.pendingReqs[msgID]
	wc.reqLock.RUnlock()

	if exists {
		// Send response to the waiting goroutine
		respCh <- msg
	}
}

func (wc *webSocketCore) Disconnect(ctx context.Context) {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	wc.cleanupConnection()
	wc.isConnected = false

	// Emit the disconnected signal
	handle := qcontext.GetHandle(ctx)
	handle.DoInMainThread(func(ctx context.Context) {
		wc.disconnected.Emit(qdata.DisconnectedArgs{
			Ctx: ctx,
			Err: nil,
		})
	})
}

func (wc *webSocketCore) cleanupConnection() {
	if wc.conn != nil {
		wc.conn.Close(websocket.StatusNormalClosure, "Client disconnecting")
		wc.conn = nil
	}

	if wc.done != nil {
		close(wc.done)
		wc.done = nil
	}

	// Close all pending request channels
	wc.reqLock.Lock()
	for id, ch := range wc.pendingReqs {
		close(ch)
		delete(wc.pendingReqs, id)
	}
	wc.reqLock.Unlock()
}

func (wc *webSocketCore) IsConnected() bool {
	wc.lock.RLock()
	defer wc.lock.RUnlock()
	return wc.isConnected
}

func (wc *webSocketCore) CheckConnection(ctx context.Context) bool {
	if !wc.IsConnected() {
		return false
	}

	wc.lock.RLock()
	conn := wc.conn
	wc.lock.RUnlock()

	if conn == nil {
		return false
	}

	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err := conn.Ping(pingCtx); err != nil {
		qlog.Error("WebSocket connection check failed: %v", err)
		wc.signalReconnect()
		return false
	}

	return true
}

func (wc *webSocketCore) signalReconnect() {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isConnected {
		wc.isConnected = false

		// This is a difficult situation because we don't have a context from the caller
		// The best approach is to use a background context but make sure we don't do
		// anything with it that would cause deadlocks
		ctx := context.Background() // Can't avoid this in this disconnect scenario
		handle := qcontext.GetHandle(ctx)
		handle.DoInMainThread(func(ctx context.Context) {
			wc.disconnected.Emit(qdata.DisconnectedArgs{
				Ctx: ctx,
				Err: errors.New("connection lost"),
			})
		})

		// Signal reconnection
		select {
		case wc.reconnectCh <- struct{}{}:
		default:
			// Channel is already signaled
		}
	}
}

func (wc *webSocketCore) Publish(ctx context.Context, msg proto.Message) error {
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

	wc.lock.RLock()
	conn := wc.conn
	isConnected := wc.isConnected
	wc.lock.RUnlock()

	if !isConnected || conn == nil {
		return errors.New("not connected")
	}

	// Use the provided context for this write operation
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		qlog.Error("WebSocket write error: %v", err)
		wc.signalReconnect()
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (wc *webSocketCore) Request(ctx context.Context, msg proto.Message) (*qprotobufs.ApiMessage, error) {
	// Create message ID for correlation
	msgID := uuid.New().String()

	apiMsg := &qprotobufs.ApiMessage{
		Header: &qprotobufs.ApiHeader{Id: msgID},
	}

	// Add authentication if available
	clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
	if clientProvider != nil {
		client := clientProvider.Client(ctx)
		if client != nil {
			session := client.GetSession(ctx)
			apiMsg.Header.AccessToken = session.AccessToken()
		}
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
	wc.reqLock.Lock()
	wc.pendingReqs[msgID] = respCh
	wc.reqLock.Unlock()

	// Clean up when done
	defer func() {
		wc.reqLock.Lock()
		delete(wc.pendingReqs, msgID)
		wc.reqLock.Unlock()
	}()

	wc.lock.RLock()
	conn := wc.conn
	isConnected := wc.isConnected
	wc.lock.RUnlock()

	if !isConnected || conn == nil {
		return nil, errors.New("not connected")
	}

	// Use the provided context for the write operation
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		qlog.Error("WebSocket write error: %v", err)
		wc.signalReconnect()
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

func (wc *webSocketCore) SetConfig(config WebSocketConfig) {
	wc.config = config
}

func (wc *webSocketCore) GetConfig() WebSocketConfig {
	return wc.config
}

func (wc *webSocketCore) Connected() qss.Signal[qdata.ConnectedArgs] {
	return wc.connected
}

func (wc *webSocketCore) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return wc.disconnected
}
