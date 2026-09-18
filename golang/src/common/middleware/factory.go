package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	queueMiddleware, err := NewQueueMiddleware(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}

	return queueMiddleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	exchageMiddleware, err := NewExchangeMiddleware(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}

	return exchageMiddleware, nil
}
