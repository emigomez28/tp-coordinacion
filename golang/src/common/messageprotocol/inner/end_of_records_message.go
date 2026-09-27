package inner

import "encoding/json"

type EndOfRecordsMessage struct {
	clientID string
}

func NewEndOfRecordsMessage(clientID string) *EndOfRecordsMessage {
	return &EndOfRecordsMessage{clientID: clientID}
}

func decodeEndOfRecordsMessage(clientID string, _ json.RawMessage) (ProtocolMessage, error) {
	return NewEndOfRecordsMessage(clientID), nil
}

func (message *EndOfRecordsMessage) ClientID() string {
	return message.clientID
}

func (message *EndOfRecordsMessage) Accept(visitor Visitor) error {
	return visitor.VisitEndOfRecords(message)
}

func (message *EndOfRecordsMessage) messageType() string {
	return _TYPE_EOF
}

func (message *EndOfRecordsMessage) encodePayload() ([]byte, error) {
	return nil, nil
}
