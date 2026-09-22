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

type Sum struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	itemsByClient  map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		itemsByClient:  map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	payload, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if payload.IsEOF {
		if err := sum.handleEndOfRecordMessage(payload.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	sum.handleDataMessage(payload.ClientID, payload.Records)
}

func (sum *Sum) handleEndOfRecordMessage(clientID string) error {
	slog.Info("Received End Of Records message")

	for _, fruitItem := range sum.itemsByClient[clientID] {
		msg, err := inner.SerializeData(clientID, []fruititem.FruitItem{fruitItem})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		err = sum.outputExchange.Send(*msg)
		if err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMsg, err := inner.SerializeEOF(clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	err = sum.outputExchange.Send(*eofMsg)
	if err != nil {
		slog.Debug("While sending message", "err", err)
		return err
	}

	delete(sum.itemsByClient, clientID)
	return nil
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	items := sum.getItemsMap(clientID)

	for _, fruitRecord := range fruitRecords {
		curr, ok := items[fruitRecord.Fruit]
		if ok {
			items[fruitRecord.Fruit] = curr.Sum(fruitRecord)
		} else {
			items[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (sum *Sum) getItemsMap(clientID string) map[string]fruititem.FruitItem {
	items, ok := sum.itemsByClient[clientID]
	if !ok {
		items = map[string]fruititem.FruitItem{}
		sum.itemsByClient[clientID] = items
	}

	return items
}
