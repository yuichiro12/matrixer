package matrixer

import (
	"sort"
	"strings"
	"time"
)

type sample struct {
	Value   float64
	GroupBy []string
}

func NewSample(f float64, groupBy ...string) sample {
	return sample{
		Value:   f,
		GroupBy: groupBy,
	}
}

func getKeyByLabels(labels []string) string {
	return strings.Join(labels, "&&&")
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

func GetHeader(cs Columns) []string {
	var columnNames []string
	for _, c := range cs {
		columnNames = append(columnNames, c.Name)
	}
	return columnNames
}

func (w *Worker) Start(rc chan<- []string, fiCh <-chan sample, cs Columns) {
	ticker := time.NewTicker(w.Interval)
	var fis []sample
	for {
		select {
		case <-ticker.C:
			m := GenerateMatrix(fis, cs)
			for i := 0; i < len(m); i++ {
				rc <- m[i]
			}
		case <-w.Done:
			ticker.Stop()
			m := GenerateMatrix(fis, cs)
			for i := 0; i < len(m); i++ {
				rc <- m[i]
			}
			close(rc)
			return
		case f := <-fiCh:
			fis = append(fis, f)
		default:
		}
	}
}

func (w *Worker) Stop() {
	close(w.Done)
}

func GenerateMatrix(s []sample, cs Columns) [][]string {
	var mat [][]string
	keys, valuesMap, labelsMap := groupByLabels(s)
	for i := 0; i < len(keys); i++ {
		mat = append(mat, generateRow(valuesMap[keys[i]], labelsMap[keys[i]], cs))
	}
	return mat
}

func groupByLabels(s []sample) ([]string, map[string][]float64, map[string][]string) {
	var keys []string
	valuesMap := make(map[string][]float64)
	labelsMap := make(map[string][]string)
	for i := 0; i < len(s); i++ {
		key := getKeyByLabels(s[i].GroupBy)
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

// returns float64 as Millisecond
func DtoF(dur time.Duration) float64 {
	return float64(dur) / float64(time.Millisecond)
}
