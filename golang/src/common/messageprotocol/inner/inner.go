package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type Payload struct {
	ClientID string
	IsEOF    bool
	Records  []fruititem.FruitItem
}

func NewPayload(clientID string, isEOF bool, records []fruititem.FruitItem) *Payload {
	return &Payload{
		ClientID: clientID,
		IsEOF:    isEOF,
		Records:  records,
	}
}

type InternalMessage struct {
	ClientID string          `json:"client_id"`
	IsEOF    bool            `json:"is_eof"`
	Records  [][]interface{} `json:"records"`
}

func NewInternalProtocolMessage(clientID string, isEOF bool, records [][]interface{}) InternalMessage {
	return InternalMessage{
		ClientID: clientID,
		IsEOF:    isEOF,
		Records:  records,
	}
}

func SerializeData(clientID string, records []fruititem.FruitItem) (*middleware.Message, error) {
	pairedRecords := toPairs(records)
	isEOF := false
	internalMessage := NewInternalProtocolMessage(clientID, isEOF, pairedRecords)
	return serialize(internalMessage)
}

func SerializeEOF(clientID string) (*middleware.Message, error) {
	pairedRecords := [][]interface{}{}
	isEOF := true
	internalMessage := NewInternalProtocolMessage(clientID, isEOF, pairedRecords)
	return serialize(internalMessage)
}

func Deserialize(message *middleware.Message) (*Payload, error) {
	var internalMessage InternalMessage
	body := []byte(message.Body)
	err := json.Unmarshal(body, &internalMessage)
	if err != nil {
		return nil, err
	}

	records, err := fromPairs(internalMessage.Records)
	if err != nil {
		return nil, err
	}

	clientID := internalMessage.ClientID
	isEOF := internalMessage.IsEOF
	return NewPayload(clientID, isEOF, records), nil
}

func serialize(internalMessage InternalMessage) (*middleware.Message, error) {
	body, err := json.Marshal(internalMessage)
	if err != nil {
		return nil, err
	}

	msg := middleware.Message{Body: string(body)}

	return &msg, nil
}

func toPairs(fruitRecords []fruititem.FruitItem) [][]interface{} {
	data := [][]interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	return data
}

func fromPairs(records [][]interface{}) ([]fruititem.FruitItem, error) {
	fruitRecords := []fruititem.FruitItem{}
	for _, fruitPair := range records {

		if len(fruitPair) != 2 {
			return nil, errors.New("Fruit pair len must be at least 2")
		}
		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return fruitRecords, nil
}
