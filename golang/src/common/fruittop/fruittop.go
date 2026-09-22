package fruittop

import (
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func BuildFruitTop(items []fruititem.FruitItem, topSize int) []fruititem.FruitItem {
	itemsToSort := make([]fruititem.FruitItem, len(items))
	copy(itemsToSort, items)
	sort.SliceStable(itemsToSort, func(i, j int) bool {
		return itemsToSort[j].Less(itemsToSort[i])
	})

	if topSize < len(itemsToSort) {
		return itemsToSort[:topSize]
	}

	return itemsToSort
}
