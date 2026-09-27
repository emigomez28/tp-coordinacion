package inner

import (
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func toPairs(fruitRecords []fruititem.FruitItem) [][]interface{} {
	data := [][]interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	return data
}

func fromPairs(records [][]interface{}) ([]fruititem.FruitItem, error) {
	fruitRecords := []fruititem.FruitItem{}
	for _, fruitPair := range records {

		if len(fruitPair) != 2 {
			return nil, errors.New("Fruit pair len must be at least 2")
		}
		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return fruitRecords, nil
}
