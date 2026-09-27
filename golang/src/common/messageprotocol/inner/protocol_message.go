package inner

const (
	_TYPE_DATA        = "data"
	_TYPE_EOF         = "eof"
	_TYPE_EMIT_TOTALS = "emit_totals"
)

type ProtocolMessage interface {
	ClientID() string
	Accept(visitor Visitor) error
	messageType() string
	encodePayload() ([]byte, error)
}
