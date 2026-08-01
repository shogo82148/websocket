package main

import (
	"context"
	"log"

	"github.com/shogo82148/websocket"
)

func main() {
	ctx := context.Background()
	conn, resp, err := websocket.Dial(ctx, "ws://localhost:8080/", nil)
	if err != nil {
		panic(err)
	}
	_ = resp
	defer conn.CloseNow()

	if err := conn.Write(ctx, websocket.MessageText, []byte("Hello WebSocket!")); err != nil {
		log.Println(err)
		return
	}
}
