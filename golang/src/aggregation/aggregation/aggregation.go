package aggregation

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruittop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

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
	itemsByClient map[string]map[string]fruititem.FruitItem
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
		itemsByClient: map[string]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	payload, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if payload.IsEOF {
		if err := aggregation.handleEndOfRecordsMessage(payload.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(payload.ClientID, payload.Records)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received End Of Records message", "clientID", clientID)

	fruitTopRecords := aggregation.buildFruitTop(clientID)
	message, err := inner.SerializeData(clientID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMsg, err := inner.SerializeEOF(clientID)
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
	fruitItems := aggregation.getItemsMap(clientID)
	for _, fruitRecord := range fruitRecords {
		curr, ok := fruitItems[fruitRecord.Fruit]
		if ok {
			fruitItems[fruitRecord.Fruit] = curr.Sum(fruitRecord)
		} else {
			fruitItems[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) getItemsMap(clientID string) map[string]fruititem.FruitItem {
	fruitItems, ok := aggregation.itemsByClient[clientID]
	if !ok {
		fruitItems = map[string]fruititem.FruitItem{}
		aggregation.itemsByClient[clientID] = fruitItems
	}

	return fruitItems
}

func (aggregation *Aggregation) buildFruitTop(clientID string) []fruititem.FruitItem {
	items := aggregation.getItemsMap(clientID)
	fruitItems := make([]fruititem.FruitItem, 0)
	for _, item := range items {
		fruitItems = append(fruitItems, item)
	}

	return fruittop.BuildFruitTop(fruitItems, aggregation.topSize)
}
