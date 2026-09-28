package join

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruittop"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type clientTops struct {
	items    map[string]fruititem.FruitItem
	eofCount int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	aggregationAmount int
	topSize           int
	topsByClient      map[string]*clientTops
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		topsByClient:      map[string]*clientTops{},
	}, nil
}

func (join *Join) Run() error {
	go join.handleSignals()
	defer join.closeMiddlewares()

	return join.inputQueue.StartConsuming(func(message middleware.Message, ack, nack func()) {
		join.handleMessage(message, ack, nack)
	})
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.inputQueue.StopConsuming()
}

func (join *Join) closeMiddlewares() {
	join.inputQueue.Close()
	join.outputQueue.Close()
}

func (join *Join) VisitData(message *inner.DataMessage) error {
	join.handleDataMessage(message.ClientID(), message.Records())

	return nil
}

func (join *Join) VisitEndOfRecords(message *inner.EndOfRecordsMessage) error {
	return join.handleEndOfRecordsMessage(message.ClientID())
}

func (join *Join) VisitEmitTotals(message *inner.EmitTotalsMessage) error {
	return inner.NewUnexpectedMessageError("join", message)
}

func (join *Join) handleMessage(message middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMessage, err := inner.Deserialize(&message)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if err := innerMessage.Accept(join); err != nil {
		slog.Error("While handling inner message", "err", err)
	}
}

func (join *Join) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	state := join.getClientTops(clientID)
	for _, fruitRecord := range fruitRecords {
		curr, ok := state.items[fruitRecord.Fruit]
		if ok {
			state.items[fruitRecord.Fruit] = curr.Sum(fruitRecord)
		} else {
			state.items[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) handleEndOfRecordsMessage(clientID string) error {
	state := join.getClientTops(clientID)
	state.eofCount++
	if state.eofCount < join.aggregationAmount {
		return nil
	}

	err := join.sendFinalTop(clientID, state)
	delete(join.topsByClient, clientID)

	return err
}

func (join *Join) getClientTops(clientID string) *clientTops {
	state, ok := join.topsByClient[clientID]
	if !ok {
		state = newClientTops()
		join.topsByClient[clientID] = state
	}

	return state
}

func newClientTops() *clientTops {
	return &clientTops{items: map[string]fruititem.FruitItem{}}
}

func (join *Join) sendFinalTop(clientID string, state *clientTops) error {
	items := make([]fruititem.FruitItem, 0)
	for _, item := range state.items {
		items = append(items, item)
	}

	topItems := fruittop.BuildFruitTop(items, join.topSize)
	dataMsg := inner.NewDataMessage(clientID, topItems)
	msg, err := inner.Serialize(dataMsg)
	if err != nil {
		return err
	}

	return join.outputQueue.Send(*msg)
}
