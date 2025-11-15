package actors

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pragyandas/hydra/actor"
	"github.com/pragyandas/hydra/actor/serializer"
	as "github.com/pragyandas/hydra/actorsystem"
)

type ChatroomState struct {
	members []string
}

type ChatroomMessage struct {
	Sender  string `json:"sender"`
	Type    string `json:"type"`
	Message string `json:"message,omitempty"`
}

func RegisterChatroomActor(actorSystem *as.ActorSystem) error {
	return actorSystem.RegisterActorType("chatroom", actor.ActorTypeConfig{
		MessageHandlerFactory: func(self *actor.Actor) actor.MessageHandler {
			return func(msg []byte) error {
				var chatroomMessage ChatroomMessage
				if err := json.Unmarshal(msg, &chatroomMessage); err != nil {
					return fmt.Errorf("failed to unmarshal chatroom message: %w", err)
				}

				switch string(chatroomMessage.Type) {
				case "join":
					{
						fmt.Println("Join request received for", chatroomMessage.Sender)
						var chatroomState ChatroomState
						state := self.GetState(context.Background())
						if state == nil {
							chatroomState = ChatroomState{members: []string{chatroomMessage.Sender}}
						} else {
							chatroomState = state.(ChatroomState)
							chatroomState.members = append(chatroomState.members, chatroomMessage.Sender)
						}
						self.SetState(context.Background(), chatroomState)
					}
				case "leave":
					{
						fmt.Println("Leave request received for", chatroomMessage.Sender)
						state := self.GetState(context.Background())
						chatroomState := state.(ChatroomState)
						for i, member := range chatroomState.members {
							if member == chatroomMessage.Sender {
								chatroomState.members = append(chatroomState.members[:i], chatroomState.members[i+1:]...)
								break
							}
						}
						self.SetState(context.Background(), chatroomState)
						userMessage := UserMessage{
							Sender:  "System",
							Message: fmt.Sprintf("%s left the chatroom", chatroomMessage.Sender),
						}
						msgBytes, err := json.Marshal(userMessage)
						if err != nil {
							fmt.Println("Failed to marshal user message", err)
						}

						for _, member := range chatroomState.members {
							fmt.Println("Sending message to", member)
							self.SendMessage("user", member, msgBytes)
						}
					}
				case "message":
					{
						state := self.GetState(context.Background())
						if state == nil {
							fmt.Println("No members in chatroom, dropping message")
							return nil
						}
						chatroomState := state.(ChatroomState)
						fmt.Println("Broadcasting message to", len(chatroomState.members), "members")
						userMessage := UserMessage{
							Sender:  chatroomMessage.Sender,
							Message: chatroomMessage.Message,
						}
						msgBytes, err := json.Marshal(userMessage)
						if err != nil {
							fmt.Println("Failed to marshal user message", err)
							return fmt.Errorf("failed to marshal user message: %w", err)
						}
						for _, member := range chatroomState.members {
							fmt.Println("Sending message to", member)
							self.SendMessage("user", member, msgBytes)
						}
					}
				}
				return nil
			}
		},
		StateSerializer: serializer.NewJSONSerializer(ChatroomState{}),
		MessageErrorHandler: func(err error, msg actor.Message) {
			fmt.Println(err)
		},
	})
}
