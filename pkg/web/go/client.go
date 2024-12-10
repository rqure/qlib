package web

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type Message = *protobufs.WebMessage

func NewMessage() Message {
	return new(protobufs.WebMessage)
}

type MessageHandler func(Client, Message)

type Client interface {
	Id() string
	Write(Message)
	Close()
	SetMessageHandler(MessageHandler)
}

type ClientImpl struct {
	id             string
	connection     *websocket.Conn
	readCh         chan Message
	wg             sync.WaitGroup
	onClose        func(string)
	isClosed       atomic.Bool
	closeMu        sync.Mutex
	messageHandler MessageHandler
	writeMu        sync.Mutex
}

func NewClient(connection *websocket.Conn, onClose func(string)) Client {
	c := &ClientImpl{
		id:         uuid.New().String(),
		connection: connection,
		readCh:     make(chan Message, 1000),
		onClose:    onClose,
	}

	go c.backgroundRead()

	return c
}

func (c *ClientImpl) Id() string {
	return c.id
}

func (c *ClientImpl) SetMessageHandler(handler MessageHandler) {
	c.messageHandler = handler
}

func (c *ClientImpl) backgroundRead() {
	defer c.Close()
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		if c.isClosed.Load() {
			return
		}

		t, b, err := c.connection.ReadMessage()

		if err != nil {
			if !c.isClosed.Load() {
				log.Error("[ClientImpl::backgroundRead] Error reading message: %v", err)
			}
			return
		}

		if t == websocket.BinaryMessage {
			m := NewMessage()
			if err := proto.Unmarshal(b, m); err != nil {
				log.Error("[ClientImpl::backgroundRead] Error unmarshalling bytes into message: %v", err)
				continue
			}

			log.Debug("[ClientImpl::backgroundRead] Received message: %v", m)
			if c.messageHandler != nil {
				c.messageHandler(c, m)
			}
		}
	}
}

func (c *ClientImpl) Write(message Message) {
	if c.isClosed.Load() {
		return
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	b, err := proto.Marshal(message)

	if err != nil {
		log.Error("[ClientImpl::Write] Error marshalling message: %v", err)
		return
	}

	if err := c.connection.WriteMessage(websocket.BinaryMessage, b); err != nil {
		log.Error("[ClientImpl::Write] Error writing message: %v", err)
	} else {
		log.Debug("[ClientImpl::Write] Sent message: %v", message)
	}
}

func (c *ClientImpl) Close() {
	if c.isClosed.Load() {
		return
	}

	c.closeMu.Lock()
	if !c.isClosed.CompareAndSwap(false, true) {
		c.closeMu.Unlock()
		return
	}
	c.closeMu.Unlock()

	if err := c.connection.Close(); err != nil {
		log.Error("[ClientImpl::Close] Error closing connection: %v", err)
	}

	close(c.readCh)
	c.wg.Wait()

	if c.onClose != nil {
		c.onClose(c.id)
	}
}
