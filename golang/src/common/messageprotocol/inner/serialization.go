package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type serializedMessage struct {
	Type     string          `json:"type"`
	ClientID string          `json:"client_id"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

type decoder struct {
	decoderFunctions map[string]func(clientID string, payload json.RawMessage) (ProtocolMessage, error)
}

func newDecoder() decoder {
	decoderFunctions := map[string]func(clientID string, paylaod json.RawMessage) (ProtocolMessage, error){
		_TYPE_DATA:        decodeDataMessage,
		_TYPE_EOF:         decodeEndOfRecordsMessage,
		_TYPE_EMIT_TOTALS: decodeEmitTotalsMessage,
	}
	return decoder{decoderFunctions: decoderFunctions}
}

func (decoder decoder) decode(serializedMessage serializedMessage) (ProtocolMessage, error) {
	decodeFn, ok := decoder.decoderFunctions[serializedMessage.Type]
	if !ok {
		return nil, NewUnknownMessageTypeError(serializedMessage.Type)
	}

	msg, err := decodeFn(serializedMessage.ClientID, serializedMessage.Payload)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func Serialize(message ProtocolMessage) (*middleware.Message, error) {
	payload, err := message.encodePayload()
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(serializedMessage{
		Type:     message.messageType(),
		ClientID: message.ClientID(),
		Payload:  payload,
	})
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func Deserialize(message *middleware.Message) (ProtocolMessage, error) {
	var serialized serializedMessage
	if err := json.Unmarshal([]byte(message.Body), &serialized); err != nil {
		return nil, err
	}
	decoder := newDecoder()
	msg, err := decoder.decode(serialized)
	if err != nil {
		return nil, err
	}

	return msg, nil
}
