package opinionatedevents

func filter[V any](collection []V, predicate func(item V, index int) bool) []V {
	result := make([]V, 0, len(collection))
	for i, item := range collection {
		if predicate(item, i) {
			result = append(result, item)
		}
	}
	return result
}

func groupIntoBatches[T any](arr []T, batchSize int) [][]T {
	if len(arr) == 0 {
		return [][]T{}
	}
	var batches [][]T
	for i := 0; i < len(arr); i += batchSize {
		end := i + batchSize
		if end > len(arr) {
			end = len(arr)
		}
		batches = append(batches, arr[i:end])
	}
	return batches
}
