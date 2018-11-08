package pinkpanther

import (
	"sort"
	"strings"
	"time"
)

type Sample struct {
	Value   float64
	GroupBy []string
}

func NewSample(f float64, groupBy ...string) Sample {
	return Sample{
		Value:   f,
		GroupBy: groupBy,
	}
}

func genKeyByGroups(groups []string) string {
	return strings.Join(groups, "&&&")
}

type Worker struct {
	Done     chan struct{}
	Interval time.Duration
}

func NewWorker(interval time.Duration) *Worker {
	return &Worker{
		Interval: interval,
		Done:     make(chan struct{}),
	}
}

func GetHeader(columns Columns) []string {
	var columnNames []string
	for _, c := range columns {
		columnNames = append(columnNames, c.Name)
	}
	return columnNames
}

func (w *Worker) Start(columns Columns, sender <-chan Sample, receiver chan<- []string) {
	ticker := time.NewTicker(w.Interval)
	var samples []Sample
	for {
		select {
		case <-ticker.C:
			m := generateMatrix(samples, columns)
			for i := 0; i < len(m); i++ {
				receiver <- m[i]
			}
		case <-w.Done:
			ticker.Stop()
			m := generateMatrix(samples, columns)
			for i := 0; i < len(m); i++ {
				receiver <- m[i]
			}
			close(receiver)
			return
		case f := <-sender:
			samples = append(samples, f)
		default:
		}
	}
}

func (w *Worker) Stop() {
	close(w.Done)
}

func generateMatrix(s []Sample, cs Columns) [][]string {
	var mat [][]string
	keys, valuesMap, labelsMap := groupByLabels(s)
	for i := 0; i < len(keys); i++ {
		mat = append(mat, generateRow(valuesMap[keys[i]], labelsMap[keys[i]], cs))
	}
	return mat
}

func groupByLabels(s []Sample) ([]string, map[string][]float64, map[string][]string) {
	var keys []string
	valuesMap := make(map[string][]float64)
	labelsMap := make(map[string][]string)
	for i := 0; i < len(s); i++ {
		key := genKeyByGroups(s[i].GroupBy)
		if _, ok := valuesMap[key]; !ok {
			keys = append(keys, key)
			labelsMap[key] = s[i].GroupBy
		}
		valuesMap[key] = append(valuesMap[key], s[i].Value)
	}
	sort.Strings(keys)
	return keys, valuesMap, labelsMap
}

func generateRow(fs []float64, labels []string, cs Columns) []string {
	var row []string
	var labelIdx int
	for i := 0; i < len(cs); i++ {
		switch cs[i].Type {
		case GROUP:
			row = append(row, labels[labelIdx])
			labelIdx++
		case STATS:
			st, _ := cs[i].Func(fs)
			row = append(row, st)
		}
	}
	return row
}
