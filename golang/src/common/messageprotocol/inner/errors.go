package inner

import "fmt"

type errorKind string

const (
	_KIND_UNKNOWN_MESSAGE_TYPE errorKind = "unknown message type"
	_KIND_UNEXPECTED_MESSAGE   errorKind = "unexpected message for this stage"
)

type ProtocolError struct {
	kind   errorKind
	detail string
}

func (protocolError *ProtocolError) Error() string {
	return fmt.Sprintf("%s: %s", protocolError.kind, protocolError.detail)
}

func NewUnknownMessageTypeError(unknownType string) error {
	return &ProtocolError{
		kind:   _KIND_UNKNOWN_MESSAGE_TYPE,
		detail: fmt.Sprintf("'%s'", unknownType),
	}
}

func NewUnexpectedMessageError(stage string, message ProtocolMessage) error {
	return &ProtocolError{
		kind:   _KIND_UNEXPECTED_MESSAGE,
		detail: fmt.Sprintf("unexpected %#v in %s, clientID '%s'", message, stage, message.ClientID()),
	}
}
