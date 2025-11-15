package main

import (
	"context"
	"log"
	"net/http"

	"github.com/gorilla/websocket"

	"github.com/pragyandas/hydra/examples/chat/messageserver"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func main() {
	ms := messageserver.Initialize(context.Background())
	defer ms.Close()

	http.HandleFunc("/messenger", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}
		defer conn.Close()

		userId := r.URL.Query().Get("userId")
		if userId == "" {
			log.Println("userId query parameter is missing")
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "userId is required"))
			return
		}
		user, err := ms.CreateUser(userId, conn)
		if err != nil {
			log.Println(err)
			return
		}
		user.JoinChatroom()
		defer user.LeaveChatroom()

		for {
			_, message, err := conn.ReadMessage()

			if err != nil {
				log.Println(err)
				return
			}
			user.OnMessage(string(message))
		}
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	http.ListenAndServe(":8080", nil)
}
