package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type DataMessage struct {
	clientID string
	records  []fruititem.FruitItem
}

func NewDataMessage(clientID string, records []fruititem.FruitItem) *DataMessage {
	return &DataMessage{
		clientID: clientID,
		records:  records,
	}
}

func decodeDataMessage(clientID string, payload json.RawMessage) (ProtocolMessage, error) {
	var pairs [][]interface{}
	if err := json.Unmarshal(payload, &pairs); err != nil {
		return nil, err
	}

	records, err := fromPairs(pairs)
	if err != nil {
		return nil, err
	}

	return NewDataMessage(clientID, records), nil
}

func (message *DataMessage) ClientID() string {
	return message.clientID
}

func (message *DataMessage) Records() []fruititem.FruitItem {
	return message.records
}

func (message *DataMessage) Accept(visitor Visitor) error {
	return visitor.VisitData(message)
}

func (message *DataMessage) messageType() string {
	return _TYPE_DATA
}

func (message *DataMessage) encodePayload() ([]byte, error) {
	pairs := toPairs(message.records)
	return json.Marshal(pairs)
}
