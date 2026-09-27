package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	ID                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type clientItems struct {
	items   map[string]fruititem.FruitItem
	emitted bool
}

type Sum struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware

	sumAmount     int
	itemsByClient map[string]*clientItems
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := createExchangeWithRoutingKeys(config.AggregationAmount, config.AggregationPrefix, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		sumAmount:      config.SumAmount,
		itemsByClient:  map[string]*clientItems{},
	}, nil
}

func createExchangeWithRoutingKeys(amount int, prefix string, connSettings middleware.ConnSettings) (middleware.Middleware, error) {
	routingKeys := make([]string, amount)
	for i := range amount {
		routingKeys[i] = fmt.Sprintf("%s_%d", prefix, i)
	}

	return middleware.CreateExchangeMiddleware(prefix, routingKeys, connSettings)
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(message middleware.Message, ack, nack func()) {
		sum.handleMessage(message, ack, nack)
	})
}

func (sum *Sum) VisitData(message *inner.DataMessage) error {
	sum.handleDataMessage(message.ClientID(), message.Records())

	return nil
}

func (sum *Sum) VisitEndOfRecords(message *inner.EndOfRecordsMessage) error {
	return sum.emitTotals(message.ClientID(), sum.sumAmount-1)
}

func (sum *Sum) VisitEmitTotals(message *inner.EmitTotalsMessage) error {
	return sum.emitTotals(message.ClientID(), 0)
}

func (sum *Sum) handleMessage(message middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMessage, err := inner.Deserialize(&message)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if err := innerMessage.Accept(sum); err != nil {
		slog.Error("While handling inner message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	state := sum.getClientItems(clientID)
	for _, fruitRecord := range fruitRecords {
		curr, ok := state.items[fruitRecord.Fruit]
		if ok {
			state.items[fruitRecord.Fruit] = curr.Sum(fruitRecord)
		} else {
			state.items[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (sum *Sum) getClientItems(clientID string) *clientItems {
	state, ok := sum.itemsByClient[clientID]
	if !ok {
		state = newClientItems()
		sum.itemsByClient[clientID] = state
	}

	return state
}

func newClientItems() *clientItems {
	return &clientItems{items: map[string]fruititem.FruitItem{}}
}

func (clientItems *clientItems) setEmitTotalsToFinished() {
	clientItems.emitted = true
	clientItems.items = nil
}

func (sum *Sum) emitTotals(clientID string, copies int) error {
	state := sum.getClientItems(clientID)
	if state.emitted {
		return sum.askPeersToEmit(clientID, copies+1)
	}

	slog.Info("Emitting totals", "clientID", clientID)

	if err := sum.sendTotals(clientID, state); err != nil {
		return err
	}
	state.setEmitTotalsToFinished()

	return sum.askPeersToEmit(clientID, copies)
}

func (sum *Sum) askPeersToEmit(clientID string, copies int) error {
	if copies <= 0 {
		return nil
	}

	emitTotalsMsg := inner.NewEmitTotalsMessage(clientID)
	msg, err := inner.Serialize(emitTotalsMsg)
	if err != nil {
		return err
	}

	for range copies {
		if err := sum.inputQueue.Send(*msg); err != nil {
			return err
		}
	}

	return nil
}

func (sum *Sum) sendTotals(clientID string, state *clientItems) error {
	for _, fruitItem := range state.items {
		records := []fruititem.FruitItem{fruitItem}
		dataMsg := inner.NewDataMessage(clientID, records)
		msg, err := inner.Serialize(dataMsg)
		if err != nil {
			return err
		}
		if err := sum.outputExchange.Send(*msg); err != nil {
			return err
		}
	}

	innerEOFMsg := inner.NewEndOfRecordsMessage(clientID)
	eofMsg, err := inner.Serialize(innerEOFMsg)
	if err != nil {
		return err
	}

	return sum.outputExchange.Send(*eofMsg)
}
