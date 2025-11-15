package actors

import (
	"context"
	"encoding/json"

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

func RegisterUserActor(ctx context.Context, actorSystem *as.ActorSystem) error {
	return actorSystem.RegisterActorType(ctx, "user", actor.ActorTypeConfig{})
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
