package qws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

const DefaultNotificationTimeout = 5 * time.Second

// WebSocketStoreNotifier implements the StoreNotifier interface
type WebSocketStoreNotifier struct {
	core            WebSocketCore
	handle          qcontext.Handle
	appName         string
	callbacks       map[string][]qdata.NotificationCallback
	callbacksMutex  sync.RWMutex
	keepAlive       *time.Ticker
	cancelKeepAlive context.CancelFunc
}

// NewStoreNotifier creates a new WebSocketStoreNotifier
func NewStoreNotifier(core WebSocketCore) qdata.StoreNotifier {
	notifier := &WebSocketStoreNotifier{
		core:      core,
		callbacks: map[string][]qdata.NotificationCallback{},
		keepAlive: time.NewTicker(30 * time.Second),
	}

	// Subscribe to connection events
	core.Connected().Connect(notifier.onConnected)
	core.Disconnected().Connect(notifier.onDisconnected)

	return notifier
}

func (wsn *WebSocketStoreNotifier) onConnected(args qdata.ConnectedArgs) {
	wsn.appName = qcontext.GetAppName(args.Ctx)
	wsn.handle = qcontext.GetHandle(args.Ctx)

	// Start keepAlive task
	ctx, cancel := context.WithCancel(context.Background())
	wsn.cancelKeepAlive = cancel
	go wsn.keepAliveTask(ctx)
}

func (wsn *WebSocketStoreNotifier) onDisconnected(args qdata.DisconnectedArgs) {
	// Stop keepAliveTask
	if wsn.cancelKeepAlive != nil {
		wsn.cancelKeepAlive()
		wsn.cancelKeepAlive = nil
	}
}

// processNotification is called when a notification is received from the server
// This will be called from the readLoop in WebSocketCore when it detects a notification message
func (wsn *WebSocketStoreNotifier) processNotification(notifPb *qprotobufs.DatabaseNotification) {
	if notifPb == nil {
		return
	}

	notif := qnotify.FromPb(notifPb)

	if wsn.handle == nil {
		qlog.Error("No handle available to process notification")
		return
	}

	wsn.handle.DoInMainThread(func(ctx context.Context) {
		wsn.callbacksMutex.RLock()
		callbacks, ok := wsn.callbacks[notif.GetToken()]
		wsn.callbacksMutex.RUnlock()

		if ok {
			for _, cb := range callbacks {
				cb.Fn(ctx, notif)
			}
		}
	})
}

func (wsn *WebSocketStoreNotifier) keepAliveTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-wsn.keepAlive.C:
			if wsn.handle == nil {
				continue
			}

			wsn.handle.DoInMainThread(func(ctx context.Context) {
				wsn.callbacksMutex.RLock()
				for token := range wsn.callbacks {
					wsn.sendNotify(ctx, qnotify.FromToken(token))
				}
				wsn.callbacksMutex.RUnlock()
			})
		}
	}
}

func (wsn *WebSocketStoreNotifier) sendNotify(ctx context.Context, config qdata.NotificationConfig) error {
	if config.GetServiceId() == "" {
		appName := qcontext.GetAppName(ctx)
		config.SetServiceId(appName)
	}

	msg := &qprotobufs.ApiRuntimeRegisterNotificationRequest{
		Requests: []*qprotobufs.DatabaseNotificationConfig{qnotify.ToConfigPb(config)},
	}

	// Set an explicit timeout for notification registration
	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultNotificationTimeout)
	defer cancel()

	resp, err := wsn.core.Request(timeoutCtx, msg)
	if err != nil {
		return err
	}

	var response qprotobufs.ApiRuntimeRegisterNotificationResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if response.Status != qprotobufs.ApiRuntimeRegisterNotificationResponse_SUCCESS {
		return fmt.Errorf("failed to register notification: %s", response.Status.String())
	}

	if len(response.Tokens) == 0 {
		return errors.New("no tokens returned")
	}

	return nil
}

// Notify registers a notification
func (wsn *WebSocketStoreNotifier) Notify(ctx context.Context, config qdata.NotificationConfig, cb qdata.NotificationCallback) (qdata.NotificationToken, error) {
	tokenId := config.GetToken()
	var err error

	wsn.callbacksMutex.Lock()
	if wsn.callbacks[tokenId] == nil {
		err = wsn.sendNotify(ctx, config)
	}

	if err == nil {
		wsn.callbacks[tokenId] = append(wsn.callbacks[tokenId], cb)
	}
	wsn.callbacksMutex.Unlock()

	if err == nil {
		return qnotify.NewToken(tokenId, wsn, cb), nil
	}

	return qnotify.NewToken("", wsn, nil), fmt.Errorf("failed to register notification: %w", err)
}

// Unnotify unregisters a notification
func (wsn *WebSocketStoreNotifier) Unnotify(ctx context.Context, token string) error {
	msg := &qprotobufs.ApiRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	}

	// Set an explicit timeout for notification unregistration
	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultNotificationTimeout)
	defer cancel()

	resp, err := wsn.core.Request(timeoutCtx, msg)
	if err != nil {
		return fmt.Errorf("failed to unregister notification: %w", err)
	}

	var response qprotobufs.ApiRuntimeUnregisterNotificationResponse
	if err := proto.Unmarshal(resp.Payload.Value, &response); err != nil {
		return err
	}

	if response.Status != qprotobufs.ApiRuntimeUnregisterNotificationResponse_SUCCESS {
		return fmt.Errorf("unregister notification failed: %s", response.Status.String())
	}

	wsn.callbacksMutex.Lock()
	delete(wsn.callbacks, token)
	wsn.callbacksMutex.Unlock()

	return nil
}

// UnnotifyCallback removes a specific callback from a notification
func (wsn *WebSocketStoreNotifier) UnnotifyCallback(ctx context.Context, token string, cb qdata.NotificationCallback) error {
	wsn.callbacksMutex.Lock()
	defer wsn.callbacksMutex.Unlock()

	if wsn.callbacks[token] == nil {
		return fmt.Errorf("no callbacks registered for token: %s", token)
	}

	callbacks := []qdata.NotificationCallback{}
	for _, callback := range wsn.callbacks[token] {
		if callback.Id() != cb.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	if len(callbacks) == 0 {
		return wsn.Unnotify(ctx, token)
	}

	wsn.callbacks[token] = callbacks
	return nil
}
