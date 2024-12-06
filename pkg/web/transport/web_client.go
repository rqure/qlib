package qweb

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type IWebClient interface {
	Id() string
	Read() *WebMessage
	Write(message *WebMessage)
	Close()
}

type WebClient struct {
	id         string
	connection *websocket.Conn
	readCh     chan *WebMessage
	wg         sync.WaitGroup
	onClose    func(string)
	isClosed   atomic.Bool
}

func NewWebClient(connection *websocket.Conn, onClose func(string)) *WebClient {
	c := &WebClient{
		id:         uuid.New().String(),
		connection: connection,
		readCh:     make(chan *WebMessage, 100),
		onClose:    onClose,
	}

	go c.backgroundRead()

	return c
}

func (c *WebClient) Id() string {
	return c.id
}

func (c *WebClient) backgroundRead() {
	defer c.Close()
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		t, b, err := c.connection.ReadMessage()

		if err != nil {
			Error("[WebClient::backgroundRead] Error reading message: %v", err)
			return
		}

		if t == websocket.BinaryMessage {
			m := new(WebMessage)
			if err := proto.Unmarshal(b, m); err != nil {
				Error("[WebClient::backgroundRead] Error unmarshalling bytes into message: %v", err)
				continue
			}

			Debug("[WebClient::backgroundRead] Received message: %v", m)
			c.readCh <- m
		}
	}
}

func (c *WebClient) Read() *WebMessage {
	select {
	case m := <-c.readCh:
		return m
	default:
		return nil
	}
}

func (c *WebClient) Write(message *WebMessage) {
	b, err := proto.Marshal(message)

	if err != nil {
		Error("[WebClient::Write] Error marshalling message: %v", err)
		return
	}

	if err := c.connection.WriteMessage(websocket.BinaryMessage, b); err != nil {
		Error("[WebClient::Write] Error writing message: %v", err)
	} else {
		Debug("[WebClient::Write] Sent message: %v", message)
	}
}

func (c *WebClient) Close() {
	if !c.isClosed.CompareAndSwap(false, true) {
		close(c.readCh)

		if err := c.connection.Close(); err != nil {
			Error("[WebClient::Close] Error closing connection: %v", err)
		}

		c.wg.Wait()

		c.onClose(c.id)
	}
}
