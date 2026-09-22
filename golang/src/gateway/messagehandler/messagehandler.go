package messagehandler

import (
	"github.com/google/uuid"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	clientID string
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientID: newClientID()}
}

func newClientID() string {
	id := uuid.New()
	return id.String()
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeData(messageHandler.clientID, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOF(messageHandler.clientID)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	payload, err := inner.Deserialize(message)
	if err != nil {
		return nil, err
	}

	if payload.ClientID != messageHandler.clientID {
		return nil, nil
	}

	return payload.Records, nil
}
