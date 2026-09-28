package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

	sumAmount         int
	aggregationAmount int
	aggregationPrefix string
	itemsByClient     map[string]*clientItems
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
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		sumAmount:         config.SumAmount,
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		itemsByClient:     map[string]*clientItems{},
	}, nil
}

func createExchangeWithRoutingKeys(amount int, prefix string, connSettings middleware.ConnSettings) (middleware.Middleware, error) {
	routingKeys := make([]string, amount)
	for i := range amount {
		routingKeys[i] = getAggregationRoutingKey(prefix, i)
	}

	return middleware.CreateExchangeMiddleware(prefix, routingKeys, connSettings)
}

func getAggregationRoutingKey(prefix string, shard int) string {
	return fmt.Sprintf("%s_%d", prefix, shard)
}

func (sum *Sum) Run() error {
	go sum.handleSignals()
	defer sum.closeMiddlewares()

	return sum.inputQueue.StartConsuming(func(message middleware.Message, ack, nack func()) {
		sum.handleMessage(message, ack, nack)
	})
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.inputQueue.StopConsuming()
}

func (sum *Sum) closeMiddlewares() {
	sum.inputQueue.Close()
	sum.outputExchange.Close()
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
	err := sum.sendRecordsByShard(clientID, state.items)
	if err != nil {
		return err
	}

	return sum.broadcastEndOfRecords(clientID)
}

func (sum *Sum) sendRecordsByShard(clientID string, items map[string]fruititem.FruitItem) error {
	recordsByShard := sum.groupByShard(items)

	for shard, records := range recordsByShard {
		if len(records) == 0 {
			continue
		}

		dataMsg := inner.NewDataMessage(clientID, records)
		msg, err := inner.Serialize(dataMsg)
		if err != nil {
			return err
		}

		prefix := sum.aggregationPrefix
		routingKey := getAggregationRoutingKey(prefix, shard)

		err = sum.outputExchange.SendTo(*msg, routingKey)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sum *Sum) broadcastEndOfRecords(clientID string) error {
	innerEOFMsg := inner.NewEndOfRecordsMessage(clientID)
	eofMsg, err := inner.Serialize(innerEOFMsg)
	if err != nil {
		return err
	}

	return sum.outputExchange.Send(*eofMsg)
}

func (sum *Sum) groupByShard(items map[string]fruititem.FruitItem) [][]fruititem.FruitItem {
	recordsByShard := make([][]fruititem.FruitItem, sum.aggregationAmount)
	for _, fruitItem := range items {
		shard := sum.getShardFor(fruitItem.Fruit)
		recordsByShard[shard] = append(recordsByShard[shard], fruitItem)
	}
	return recordsByShard
}

func (sum *Sum) getShardFor(fruit string) int {
	hash := fnv.New32a()
	fruitAsBytes := []byte(fruit)
	hash.Write(fruitAsBytes)

	aggAmount := uint32(sum.aggregationAmount)
	shard := int(hash.Sum32() % aggAmount)
	return shard
}
