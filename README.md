# websocket

WebSocket library for Go

## Install

```bash
go get github.com/shogo82148/websocket@latest
```

## Examples

The API is compatible with <https://github.com/coder/websocket>.

### Server

```go
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

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    if err := conn.Write(ctx, websocket.MessageText, []byte("Hello WebSocket!")); err != nil {
      log.Println(err)
      return
    }

    if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
      log.Println(err)
      return
    }
  })
  http.ListenAndServe(":8080", nil)
}
```

### Client

```go
package main

import (
  "context"
  "log"

  "github.com/shogo82148/websocket"
)

func main() {
  ctx := context.Background()
  conn, _, err := websocket.Dial(ctx, "ws://localhost:8080/", nil)
  if err != nil {
    log.Fatal(err)
  }
  defer conn.CloseNow()

  _, data, err := conn.Read(ctx)
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("Received: %s", data)

  if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
    log.Println(err)
    return
  }
}
```
