package workers

import (
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Web struct {
	ClientConnected    signalslots.Signal
	ClientDisconnected signalslots.Signal
	Received           signalslots.Signal

	clients map[string]web.Client
	addr    string

	handle app.Handle
}

func NewWeb(addr string) *Web {
	return &Web{
		clients:            make(map[string]web.Client),
		addr:               addr,
		ClientConnected:    signal.New(),
		ClientDisconnected: signal.New(),
		Received:           signal.New(),
	}
}

func (w *Web) Init(h app.Handle) {
	w.handle = h

	// Serve static files from the "static" directory
	http.Handle("/css/", http.StripPrefix("/css/", http.FileServer(http.Dir("./web/css"))))
	http.Handle("/img/", http.StripPrefix("/img/", http.FileServer(http.Dir("./web/img"))))
	http.Handle("/js/", http.StripPrefix("/js/", http.FileServer(http.Dir("./web/js"))))

	// Handle WebSocket and other routes
	http.Handle("/", w)

	web.Register_web_handler_app()
	web.Register_web_handler_server()
	web.Register_web_handler_utils()
	web.Register_web_handler_store()

	go func() {
		err := http.ListenAndServe(w.addr, nil)
		if err != nil {
			log.Panic("HTTP server error: %v", err)
		}
	}()
}

func (w *Web) onIndexRequest(wr http.ResponseWriter, _ *http.Request) {
	index, err := os.ReadFile("web/index.html")

	if err != nil {
		log.Error("Error reading index.html: %v", err)
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
		log.Error("Error upgrading to WebSocket: %v", err)
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
	// Empty as processing is now done via handlers
}

func (w *Web) Send(clientId string, p *anypb.Any) {
	if client, ok := w.clients[clientId]; ok {
		client.Write(&protobufs.WebMessage{
			Header: &protobufs.WebHeader{
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
	client := web.NewClient(conn, func(id string) {
		w.handle.DoInMainThread(func() {
			w.clients[id].Close()
			w.ClientDisconnected.Emit(id)
			delete(w.clients, id)
		})
	})

	client.SetMessageHandler(func(c web.Client, m web.Message) {
		w.handle.DoInMainThread(func() {
			w.Received.Emit(c, m)
		})
	})

	w.handle.DoInMainThread(func() {
		w.clients[client.Id()] = client
		w.ClientConnected.Emit(client)
	})

	return client
}
