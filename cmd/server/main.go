package main

import (
	"log"
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
		if err := conn.Write(ctx, websocket.MessageText, []byte("Hello WebSocket!")); err != nil {
			log.Println(err)
			return
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("Received: %s", data)

		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			log.Println(err)
			return
		}
	})
	http.ListenAndServe(":8080", nil)
}
