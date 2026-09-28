package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruittop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type clientItems struct {
	items    map[string]fruititem.FruitItem
	eofCount int
}

type AggregationConfig struct {
	ID                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	itemsByClient map[string]*clientItems
	sumAmount     int
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.ID)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		itemsByClient: map[string]*clientItems{},
		sumAmount:     config.SumAmount,
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() error {
	go aggregation.handleSignals()
	defer aggregation.closeMiddlewares()

	return aggregation.inputExchange.StartConsuming(func(message middleware.Message, ack, nack func()) {
		aggregation.handleMessage(message, ack, nack)
	})
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.inputExchange.StopConsuming()
}

func (aggregation *Aggregation) closeMiddlewares() {
	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
}

func (aggregation *Aggregation) VisitData(message *inner.DataMessage) error {
	aggregation.handleDataMessage(message.ClientID(), message.Records())

	return nil
}

func (aggregation *Aggregation) VisitEndOfRecords(message *inner.EndOfRecordsMessage) error {
	return aggregation.handleEndOfRecordsMessage(message.ClientID())
}

func (aggregation *Aggregation) VisitEmitTotals(message *inner.EmitTotalsMessage) error {
	return inner.NewUnexpectedMessageError("aggregation", message)
}

func (aggregation *Aggregation) handleMessage(message middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMessage, err := inner.Deserialize(&message)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if err := innerMessage.Accept(aggregation); err != nil {
		slog.Error("While handling inner message", "err", err)
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received End Of Records message", "clientID", clientID)

	state := aggregation.getClientItems(clientID)
	state.eofCount++

	if state.eofCount < aggregation.sumAmount {
		return nil
	}
	fruitTopRecords := aggregation.buildFruitTop(state)
	topMsg, err := inner.Serialize(inner.NewDataMessage(clientID, fruitTopRecords))
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*topMsg); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMsg, err := inner.Serialize(inner.NewEndOfRecordsMessage(clientID))
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*eofMsg); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(aggregation.itemsByClient, clientID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	state := aggregation.getClientItems(clientID)
	for _, fruitRecord := range fruitRecords {
		curr, ok := state.items[fruitRecord.Fruit]
		if ok {
			state.items[fruitRecord.Fruit] = curr.Sum(fruitRecord)
		} else {
			state.items[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) getClientItems(clientID string) *clientItems {
	state, ok := aggregation.itemsByClient[clientID]
	if !ok {
		state = newClientItems()
		aggregation.itemsByClient[clientID] = state
	}

	return state
}

func newClientItems() *clientItems {
	return &clientItems{items: map[string]fruititem.FruitItem{}}
}

func (aggregation *Aggregation) buildFruitTop(state *clientItems) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0)
	for _, item := range state.items {
		fruitItems = append(fruitItems, item)
	}

	return fruittop.BuildFruitTop(fruitItems, aggregation.topSize)
}
