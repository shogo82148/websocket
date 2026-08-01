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

	_, data, err := conn.Read(ctx)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("Received: %s", data)

	if err := conn.Write(ctx, websocket.MessageText, []byte("Hello WebSocket!")); err != nil {
		log.Println(err)
		return
	}
}
