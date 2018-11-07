package tests

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/yuichiro12/matrixer"
)

func TestBatch(t *testing.T) {
	mc := make(chan [][]string)
	fc := make(chan matrixer.Float64Item)
	cs := matrixer.GetDefaultColumnsWithLoggedAt()
	go matrixer.NewWorker().BatchGenerateMatrix(mc, fc, cs)
	go matrixLogger(os.Stdout, cs, mc)
	for {
		fc <- matrixer.NewFloat64Item(rand.Float64())
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBatchWithOptions(t *testing.T) {
	mc := make(chan [][]string)
	fc := make(chan matrixer.Float64Item)
	worker := matrixer.NewWorker()
	worker.Interval = 5 * time.Second
	cs := matrixer.GetDefaultStatColumns()
	cs = append(cs, matrixer.Column{
		Name: "p66",
		Func: func(fs []float64) (string, error) {
			f, _ := stats.Percentile(fs, 66)
			return fmt.Sprintf("%f", f), nil
		},
	})
	cs = append(cs, matrixer.Column{
		Name: "p33",
		Func: func(fs []float64) (string, error) {
			f, _ := stats.Percentile(fs, 33)
			return fmt.Sprintf("%f", f), nil
		},
	})
	cs.AddPrefix("test_")
	cs = append(matrixer.GetLabelColumns("fruits", "animals"), cs...)
	go worker.BatchGenerateMatrix(mc, fc, cs)
	go matrixLogger(os.Stdout, cs, mc)

	for {
		fc <- matrixer.NewFloat64Item(rand.Float64(), getRandomLabel1(), getRandomLabel2())
		time.Sleep(10 * time.Millisecond)
	}
}

func getRandomLabel1() string {
	return []string{"apple", "banana", "cherry"}[rand.Intn(3)]
}
func getRandomLabel2() string {
	return []string{"dog", "elephant", "flamingo"}[rand.Intn(3)]
}

func matrixLogger(w io.Writer, cs matrixer.Columns, mc <-chan [][]string) {
	var columnNames []string
	for _, c := range cs {
		columnNames = append(columnNames, c.Name)
	}
	for m := range mc {
		var s []string
		for i := 0; i < len(columnNames); i++ {
			columnNames[i] = fmt.Sprintf("%12v", columnNames[i])
		}
		w.Write([]byte(strings.Join(columnNames, ",") + "\n"))
		for i := 0; i < len(m); i++ {
			for j := 0; j < len(m[i]); j++ {
				m[i][j] = fmt.Sprintf("%12v", m[i][j])
			}
			s = append(s, strings.Join(m[i], ","))
		}
		w.Write([]byte(strings.Join(s, "\n") + "\n"))
	}
}
