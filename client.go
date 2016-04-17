package main

import (
	"github.com/gorilla/websocket"
)

// client = chat user(once)

type client struct {
	// WebSocket
	socket *websocket.Conn
	// message channel
	send chan []byte
	// chat room
	room *room
}

func (c *client) read() {
	for {
		if _, msg, err := c.socket.ReadMessage(); err == nil {
			c.room.forward <- msg
		} else {
			break
		}

	}
	c.socket.Close()
}

func (c *client) write() {
	for msg := range c.send {
		if err := c.socket.WriteMessage(websocket.TextMessage, msg); err != nil {
			break
		}
	}
	c.socket.Close()
}
