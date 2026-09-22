package join

import (
	"log/slog"

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

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	payload, err := inner.Deserialize(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	state := join.getClientTops(payload.ClientID)

	if !payload.IsEOF {
		for _, item := range payload.Records {
			curr, ok := state.items[item.Fruit]
			if ok {
				state.items[item.Fruit] = curr.Sum(item)
			} else {
				state.items[item.Fruit] = item
			}
		}
		return
	}

	state.eofCount++
	if state.eofCount < join.aggregationAmount {
		return
	}
	err = join.sendFinalTop(payload.ClientID, state)
	if err != nil {
		slog.Error("While sending final top", "err", err)
	}

	delete(join.topsByClient, payload.ClientID)
}

func (join *Join) getClientTops(clientID string) *clientTops {
	state, ok := join.topsByClient[clientID]
	if !ok {
		state = &clientTops{
			items: map[string]fruititem.FruitItem{},
		}
		join.topsByClient[clientID] = state
	}

	return state
}

func (join *Join) sendFinalTop(clientID string, state *clientTops) error {
	items := make([]fruititem.FruitItem, 0)
	for _, item := range state.items {
		items = append(items, item)
	}

	topItems := fruittop.BuildFruitTop(items, join.topSize)
	msg, err := inner.SerializeData(clientID, topItems)
	if err != nil {
		return err
	}

	return join.outputQueue.Send(*msg)
}
