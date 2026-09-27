package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
)

type resultVisitor struct {
	clientID string
	records  []fruititem.FruitItem
}

func newResultVisitor(clientID string) *resultVisitor {
	return &resultVisitor{clientID: clientID}
}

func (visitor *resultVisitor) VisitData(message *inner.DataMessage) error {
	if message.ClientID() != visitor.clientID {
		return nil
	}

	visitor.records = message.Records()

	return nil
}

func (visitor *resultVisitor) VisitEndOfRecords(message *inner.EndOfRecordsMessage) error {
	return nil
}

func (visitor *resultVisitor) VisitEmitTotals(message *inner.EmitTotalsMessage) error {
	return nil
}
