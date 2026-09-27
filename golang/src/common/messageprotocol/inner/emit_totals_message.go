package inner

import "encoding/json"

type EmitTotalsMessage struct {
	clientID string
}

func NewEmitTotalsMessage(clientID string) *EmitTotalsMessage {
	return &EmitTotalsMessage{clientID: clientID}
}

func decodeEmitTotalsMessage(clientID string, _ json.RawMessage) (ProtocolMessage, error) {
	return NewEmitTotalsMessage(clientID), nil
}

func (message *EmitTotalsMessage) ClientID() string {
	return message.clientID
}

func (message *EmitTotalsMessage) Accept(visitor Visitor) error {
	return visitor.VisitEmitTotals(message)
}

func (message *EmitTotalsMessage) messageType() string {
	return _TYPE_EMIT_TOTALS
}

func (message *EmitTotalsMessage) encodePayload() ([]byte, error) {
	return nil, nil
}
