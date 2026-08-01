package main

import (
	"net/http"

	"github.com/shogo82148/websocket"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		ctx := r.Context()
		writer, err := conn.Writer(ctx, websocket.MessageText)
		if err != nil {
			return
		}
		_, err = writer.Write([]byte("Hello WebSocket!"))
		if err != nil {
			return
		}
	})
	http.ListenAndServe(":8080", nil)
}
