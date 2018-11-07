package matrixer

import (
	"strings"
	"time"
)

type Float64Item struct {
	Value  float64
	Labels []string
}

func NewFloat64Item(f float64, labels ...string) Float64Item {
	return Float64Item{
		Value:  f,
		Labels: labels,
	}
}

func getKeyByLabels(labels []string) string {
	return strings.Join(labels, "&&&")
}

type Worker struct {
	Done     chan struct{}
	Interval time.Duration
}

func NewWorker() Worker {
	return Worker{
		Interval: 10 * time.Second,
		Done:     make(chan struct{}),
	}
}

func GetHeader(cs Columns) []string {
	var columnNames []string
	for _, c := range cs {
		columnNames = append(columnNames, c.Name)
	}
	return columnNames
}

func (w Worker) BatchGenerateMatrix(mc chan<- [][]string, fiCh <-chan Float64Item, cs Columns) {
	ticker := time.NewTicker(w.Interval)
	var fis []Float64Item
	for {
		select {
		case <-ticker.C:
			mc <- generateMatrix(fis, cs)
		case <-w.Done:
			ticker.Stop()
			mc <- generateMatrix(fis, cs)
			close(mc)
			return
		case f := <-fiCh:
			fis = append(fis, f)
		default:
		}
	}
}

func generateMatrix(fis []Float64Item, cs Columns) [][]string {
	var mat [][]string
	keys, valuesMap, labelsMap := groupByLabels(fis)
	for i := 0; i < len(keys); i++ {
		mat = append(mat, generateRow(valuesMap[keys[i]], labelsMap[keys[i]], cs))
	}
	return mat
}

func groupByLabels(fis []Float64Item) ([]string, map[string][]float64, map[string][]string) {
	var keys []string
	valuesMap := make(map[string][]float64)
	labelsMap := make(map[string][]string)
	for i := 0; i < len(fis); i++ {
		key := getKeyByLabels(fis[i].Labels)
		if _, ok := valuesMap[key]; !ok {
			keys = append(keys, key)
			labelsMap[key] = fis[i].Labels
		}
		valuesMap[key] = append(valuesMap[key], fis[i].Value)
	}
	return keys, valuesMap, labelsMap
}

func generateRow(fs []float64, labels []string, cs Columns) []string {
	var row []string
	var labelIdx int
	for i := 0; i < len(cs); i++ {
		switch cs[i].Type {
		case LABEL:
			row = append(row, labels[labelIdx])
			labelIdx++
		case STATS:
			st, _ := cs[i].Func(fs)
			row = append(row, st)
		}
	}
	return row
}

// returns float64 as Millisecond
func DtoF(dur time.Duration) float64 {
	return float64(dur) / float64(time.Millisecond)
}
