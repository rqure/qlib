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

type Client interface {
	Id() string
	Read() Message
	Write(Message)
	Close()
}

type ClientImpl struct {
	id         string
	connection *websocket.Conn
	readCh     chan Message
	wg         sync.WaitGroup
	onClose    func(string)
	isClosed   atomic.Bool
	closeMu    sync.Mutex // Add mutex for thread-safe closure
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

func (c *ClientImpl) backgroundRead() {
	defer c.Close()
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		t, b, err := c.connection.ReadMessage()

		if err != nil {
			log.Error("[ClientImpl::backgroundRead] Error reading message: %v", err)
			return
		}

		if t == websocket.BinaryMessage {
			m := NewMessage()
			if err := proto.Unmarshal(b, m); err != nil {
				log.Error("[ClientImpl::backgroundRead] Error unmarshalling bytes into message: %v", err)
				continue
			}

			log.Debug("[ClientImpl::backgroundRead] Received message: %v", m)
			c.readCh <- m
		}
	}
}

func (c *ClientImpl) Read() Message {
	// Remove select-default to make this blocking
	return <-c.readCh
}

func (c *ClientImpl) Write(message Message) {
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
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	// Invert the condition - we want to proceed if NOT already closed
	if c.isClosed.CompareAndSwap(false, true) {
		if err := c.connection.Close(); err != nil {
			log.Error("[ClientImpl::Close] Error closing connection: %v", err)
		}

		close(c.readCh)
		c.wg.Wait()
		c.onClose(c.id)
	}
}
