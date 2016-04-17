package main

import (
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"trace"
)

type room struct {
	// 他のクライアントに転送するメッセージ
	forward chan []byte
	// チャットルームに参加しようとしているクライアントのchannel
	join chan *client
	// チャットルームから退室しようとしているクライアントのchannel
	leave chan *client
	// 在室しているすべてのクライアント
	clients map[*client]bool
	// tracer(操作ログ)
	tracer trace.Tracer
}

func newRoom() *room {
	return &room{
		forward: make(chan []byte),
		join:    make(chan *client),
		leave:   make(chan *client),
		clients: make(map[*client]bool),
		tracer:  trace.Off(),
	}
}

func (r *room) run() {
	for {
		select {
		case client := <-r.join:
			// 参加
			r.clients[client] = true
			r.tracer.Trace("新しいクライアントが来ました")
		case client := <-r.leave:
			// 退室
			delete(r.clients, client)
			close(client.send)
			r.tracer.Trace("クライアントが帰りました")
		case msg := <-r.forward:
			r.tracer.Trace("メッセージを受信しました：", string(msg))
			// メッセージ転送
			for client := range r.clients {
				select {
				case client.send <- msg:
					// メッセージ送信
					r.tracer.Trace(" -- クライアントに送信しました")
				default:
					// 送信失敗
					delete(r.clients, client)
					close(client.send)
					r.tracer.Trace(" -- 送信に失敗しました。クリーンアップします。")
				}
			}
		}
	}
}

const (
	socketBufferSize  = 1024
	messageBufferSize = 256
)

var upgrader = &websocket.Upgrader{ReadBufferSize: socketBufferSize, WriteBufferSize: socketBufferSize}

func (r *room) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	socket, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Fatal("ServeHTTP:", err)
		return
	}
	client := &client{
		socket: socket,
		send:   make(chan []byte, messageBufferSize),
		room:   r,
	}
	r.join <- client
	defer func() { r.leave <- client }()
	go client.write()
	client.read()
}
