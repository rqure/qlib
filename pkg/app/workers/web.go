package workers

import (
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rqure/qlib/pkg/signalslots"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RecievePackage struct {
	Msg web.Message
	Cli web.Client
}

type Web struct {
	ClientConnected    signalslots.Signal[web.Client]
	ClientDisconnected signalslots.Signal[string]
	Received           signalslots.Signal[RecievePackage]

	clients        map[string]web.Client
	addClientCh    chan web.Client
	removeClientCh chan string
	addr           string
}

func NewWebServiceWorker(addr string) *Web {
	return &Web{
		clients:        make(map[string]web.Client),
		addClientCh:    make(chan web.Client, 100),
		removeClientCh: make(chan string, 100),
		addr:           addr,
	}
}

func (w *Web) Init() {
	// Serve static files from the "static" directory
	http.Handle("/css/", http.StripPrefix("/css/", http.FileServer(http.Dir("./web/css"))))
	http.Handle("/img/", http.StripPrefix("/img/", http.FileServer(http.Dir("./web/img"))))
	http.Handle("/js/", http.StripPrefix("/js/", http.FileServer(http.Dir("./web/js"))))

	// Handle WebSocket and other routes
	http.Handle("/", w)

	Register_web_handler_app()
	Register_web_handler_server_interactor()
	Register_web_handler_utils()
	Register_web_handler_database_interactor()

	go func() {
		err := http.ListenAndServe(w.addr, nil)
		if err != nil {
			Panic("[WebServiceWorker::Init] HTTP server error: %v", err)
		}
	}()
}

func (w *Web) onIndexRequest(wr http.ResponseWriter, _ *http.Request) {
	index, err := os.ReadFile("web/index.html")

	if err != nil {
		Error("[WebServiceWorker::onIndexRequest] Error reading index.html: %v", err)
		return
	}

	wr.Header().Set("Content-Type", "text/html")
	wr.Write(index)
}

func (w *Web) onWSRequest(wr http.ResponseWriter, req *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(wr, req, nil)
	if err != nil {
		Error("[WebServiceWorker::onWSRequest] Error upgrading to WebSocket: %v", err)
		return
	}

	w.addClient(conn)
}

func (w *Web) ServeHTTP(wr http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/" {
		w.onIndexRequest(wr, req)
	} else if req.URL.Path == "/ws" {
		w.onWSRequest(wr, req)
	} else {
		http.NotFound(wr, req)
	}
}

func (w *Web) Deinit() {
	for _, client := range w.clients {
		client.Close()
	}
}

func (w *Web) DoWork() {
	w.processClientConnectionEvents()
	w.processClientMessages()
}

func (w *Web) processClientMessages() {
	for _, client := range w.clients {
		for {
			if m := client.Read(); m != nil {
				w.Signals.Received.Emit(client, m)
			} else {
				break
			}
		}
	}
}

func (w *Web) processClientConnectionEvents() {
	for {
		select {
		case client := <-w.addClientCh:
			Info("[WebServiceWorker::processClientConnectionEvents] Client connected: %s", client.Id())
			w.clients[client.Id()] = client
			w.Signals.ClientConnected.Emit(client)
		case id := <-w.removeClientCh:
			Info("[WebServiceWorker::processClientConnectionEvents] Client disconnected: %s", id)
			w.Signals.ClientDisconnected.Emit(id)
			delete(w.clients, id)
		default:
			return
		}
	}
}

func (w *Web) Send(clientId string, p *anypb.Any) {
	if client, ok := w.clients[clientId]; ok {
		client.Write(&WebMessage{
			Header: &WebHeader{
				Id:        uuid.New().String(),
				Timestamp: timestamppb.Now(),
			},
			Payload: p,
		})
	}
}

func (w *Web) Broadcast(p *anypb.Any) {
	for clientId := range w.clients {
		w.Send(clientId, p)
	}
}

func (w *Web) addClient(conn *websocket.Conn) web.Client {
	client := NewWebClient(conn, func(id string) {
		w.removeClientCh <- id
	})

	w.addClientCh <- client

	return client
}
