package actors

import (
	"encoding/json"
	"fmt"

	"github.com/pragyandas/hydra/actor"
	as "github.com/pragyandas/hydra/actorsystem"
)

type UserMessage struct {
	Sender  string `json:"sender"`
	Message string `json:"message"`
}

type User struct {
	ID    string
	actor *actor.Actor
}

func RegisterUserActor(actorSystem *as.ActorSystem) error {
	return actorSystem.RegisterActorType("user", actor.ActorTypeConfig{
		MessageHandlerFactory: func(self *actor.Actor) actor.MessageHandler {
			return func(msg []byte) error {
				fmt.Println("Message received by dummy handler", string(msg))
				return nil
			}
		},
		MessageErrorHandler: func(err error, msg actor.Message) {
			fmt.Println("Message received by error handler", string(msg.Data()))
			fmt.Println(err)
		},
	})
}

func CreateUser(id string, actorsystem *as.ActorSystem, messageHandler actor.MessageHandler) (*User, error) {
	userActor, err := actorsystem.GetOrCreateActor("user", id)
	if err != nil {
		return nil, err
	}

	userActor = userActor.WithMessageHandler(messageHandler)

	return &User{
		ID:    id,
		actor: userActor,
	}, nil
}

func (user *User) JoinChatroom() {
	message := ChatroomMessage{
		Sender: user.ID,
		Type:   "join",
	}
	msgBytes, _ := json.Marshal(message)
	user.actor.SendMessage("chatroom", "1", msgBytes)
}

func (user *User) LeaveChatroom() {
	message := ChatroomMessage{
		Sender: user.ID,
		Type:   "leave",
	}
	msgBytes, _ := json.Marshal(message)
	user.actor.SendMessage("chatroom", "1", msgBytes)
}

func (user *User) OnMessage(msg string) {
	message := ChatroomMessage{
		Sender:  user.ID,
		Type:    "message",
		Message: msg,
	}
	msgBytes, _ := json.Marshal(message)
	user.actor.SendMessage("chatroom", "1", msgBytes)
}
