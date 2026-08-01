package main

import (
	"context"
	"time"

	"github.com/shogo82148/websocket"
)

func main() {
	conn, resp, err := websocket.Dial(context.Background(), "ws://localhost:8080/", nil)
	if err != nil {
		panic(err)
	}
	_ = resp
	defer conn.CloseNow()

	time.Sleep(time.Second)
}
