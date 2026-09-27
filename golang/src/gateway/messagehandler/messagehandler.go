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
	records := []fruititem.FruitItem{fruitRecord}
	dataMsg := inner.NewDataMessage(messageHandler.clientID, records)

	return inner.Serialize(dataMsg)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	eofMsg := inner.NewEndOfRecordsMessage(messageHandler.clientID)

	return inner.Serialize(eofMsg)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	innerMessage, err := inner.Deserialize(message)
	if err != nil {
		return nil, err
	}

	visitor := newResultVisitor(messageHandler.clientID)
	if err := innerMessage.Accept(visitor); err != nil {
		return nil, err
	}

	return visitor.records, nil
}
