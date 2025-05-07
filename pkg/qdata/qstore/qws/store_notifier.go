package qws

import (
	"context"
	"errors"
	"fmt"

	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

// WebSocketStoreNotifier implements the StoreNotifier interface
type WebSocketStoreNotifier struct {
	core      WebSocketCore
	handle    qcontext.Handle
	appName   string
	callbacks map[string][]qdata.NotificationCallback
}

// NewStoreNotifier creates a new WebSocketStoreNotifier
func NewStoreNotifier(core WebSocketCore) qdata.StoreNotifier {
	notifier := &WebSocketStoreNotifier{
		core:      core,
		callbacks: map[string][]qdata.NotificationCallback{},
	}

	// Subscribe to connection events
	core.Connected().Connect(notifier.onConnected)
	core.Disconnected().Connect(notifier.onDisconnected)
	core.EventMsg().Connect(notifier.OnEventMsg)

	return notifier
}

func (wsn *WebSocketStoreNotifier) onConnected(args qdata.ConnectedArgs) {
	wsn.appName = qcontext.GetAppName(args.Ctx)
	wsn.handle = qcontext.GetHandle(args.Ctx)
}

func (wsn *WebSocketStoreNotifier) onDisconnected(args qdata.DisconnectedArgs) {
	wsn.callbacks = map[string][]qdata.NotificationCallback{}
}

func (wsn *WebSocketStoreNotifier) OnEventMsg(msg *qprotobufs.ApiMessage) {
	if msg == nil || msg.Payload == nil {
		return
	}

	if !msg.Payload.MessageIs(&qprotobufs.DatabaseNotification{}) {
		return
	}

	notifPb := &qprotobufs.DatabaseNotification{}
	if err := proto.Unmarshal(msg.Payload.Value, notifPb); err != nil {
		qlog.Error("failed to unmarshal notification: %v", err)
		return
	}

	wsn.onNotification(notifPb)
}

func (wsn *WebSocketStoreNotifier) onNotification(notifPb *qprotobufs.DatabaseNotification) {
	notif := qnotify.FromPb(notifPb)

	wsn.handle.DoInMainThread(func(ctx context.Context) {
		callbacks, ok := wsn.callbacks[notif.GetToken()]

		if ok {
			for _, cb := range callbacks {
				cb.Fn(ctx, notif)
			}
		}
	})
}

func (wsn *WebSocketStoreNotifier) sendNotify(ctx context.Context, config qdata.NotificationConfig) error {
	if config.GetServiceId() == "" {
		appName := qcontext.GetAppName(ctx)
		config.SetServiceId(appName)
	}

	msg := &qprotobufs.ApiRuntimeRegisterNotificationRequest{
		Requests: []*qprotobufs.DatabaseNotificationConfig{qnotify.ToConfigPb(config)},
	}

	resp, err := wsn.core.Request(ctx, msg)
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

	if wsn.callbacks[tokenId] == nil {
		err = wsn.sendNotify(ctx, config)
	}

	if err == nil {
		wsn.callbacks[tokenId] = append(wsn.callbacks[tokenId], cb)
		return qnotify.NewToken(tokenId, wsn, cb), nil
	}

	return qnotify.NewToken("", wsn, nil), fmt.Errorf("failed to register notification: %w", err)
}

// Unnotify unregisters a notification
func (wsn *WebSocketStoreNotifier) Unnotify(ctx context.Context, token string) error {
	msg := &qprotobufs.ApiRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	}

	resp, err := wsn.core.Request(ctx, msg)
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

	delete(wsn.callbacks, token)

	return nil
}

// UnnotifyCallback removes a specific callback from a notification
func (wsn *WebSocketStoreNotifier) UnnotifyCallback(ctx context.Context, token string, cb qdata.NotificationCallback) error {
	if wsn.callbacks[token] == nil {
		return fmt.Errorf("no callbacks registered for token: %s", token)
	}

	callbacks := []qdata.NotificationCallback{}
	for _, callback := range wsn.callbacks[token] {
		if callback.Id() != cb.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	wsn.callbacks[token] = callbacks
	if len(callbacks) == 0 {
		return wsn.Unnotify(ctx, token)
	}

	return nil
}
