package pinkpanther

import (
	"io"
	"log"
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
	errChan  chan<- error
}

func NewWorker(interval time.Duration, errChan chan<- error) *Worker {
	return &Worker{
		Interval: interval,
		Done:     make(chan struct{}),
		errChan:  errChan,
	}
}

func GetHeader(columns Columns) []string {
	var columnNames []string
	for _, c := range columns {
		columnNames = append(columnNames, c.Name)
	}
	return columnNames
}

func (w *Worker) Start(columns Columns, sender <-chan Sample, receiver chan<- [][]string) {
	ticker := time.NewTicker(w.Interval)
	var samples []Sample
	for {
		select {
		case <-ticker.C:
			m, err := generateMatrix(samples, columns)
			if err != nil {
				w.errChan <- err
			}
			receiver <- m
			samples = nil
		case <-w.Done:
			ticker.Stop()
			m, err := generateMatrix(samples, columns)
			if err != nil {
				w.errChan <- err
			}
			for i := 0; i < len(m); i++ {
				receiver <- m
			}
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

func generateMatrix(s []Sample, cs Columns) ([][]string, error) {
	var mat [][]string
	keys, valuesMap, labelsMap := groupByLabels(s)
	for i := 0; i < len(keys); i++ {
		row, err := generateRow(valuesMap[keys[i]], labelsMap[keys[i]], cs)
		if err != nil {
			return mat, err
		}
		mat = append(mat, row)
	}
	return mat, nil
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

func generateRow(fs []float64, labels []string, cs Columns) ([]string, error) {
	var row []string
	var labelIdx int
	for i := 0; i < len(cs); i++ {
		switch cs[i].Type {
		case GROUP:
			row = append(row, labels[labelIdx])
			labelIdx++
		case STATS:
			st, err := cs[i].Func(fs)
			if err != nil {
				return row, err
			}
			row = append(row, st)
		}
	}
	return row, nil
}

type Logger struct {
	w         io.Writer
	errChan   chan error
	Separator string
}

func NewLogger(w io.Writer, errChan chan error, sep string) Logger {
	return Logger{
		w:         w,
		errChan:   errChan,
		Separator: sep,
	}
}

func (l Logger) LogRows(rc <-chan [][]string) {
	for rs := range rc {
		for i := 0; i < len(rs); i++ {
			l.LogRow(rs[i])
		}
	}
}

func (l Logger) LogRow(r []string) {
	if _, err := l.w.Write([]byte(strings.Join(r, l.Separator) + "\n")); err != nil {
		l.errChan <- err
	}
}

func LogError(w io.Writer, ec <-chan error) {
	for e := range ec {
		if _, err := w.Write([]byte(e.Error() + "\n")); err != nil {
			log.Fatalln(err)
		}
	}
}
