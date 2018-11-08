package pinkpanther

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/yuichiro12/silkroad"
	"github.com/montanaflynn/stats"
)

func TestBatch(t *testing.T) {
	rc := make(chan []string)
	ec := make(chan error)
	fc := make(chan Sample)
	cs := GetDefaultColumnsWithLoggedAt()
	go NewWorker(10*time.Second).Start(rc, fc, cs)
	go silkroad.NewLogger(",", "\n").LogRow(os.Stdout, rc, ec)
	go silkroad.LogError(os.Stderr, ec)
	for {
		fc <- NewSample(rand.Float64())
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBatchWithOptions(t *testing.T) {
	rc := make(chan []string)
	fc := make(chan Sample)
	cs := GetDefaultStatColumns()
	cs = append(cs, &Column{
		Name: "p66",
		Func: func(fs []float64) (string, error) {
			f, _ := stats.Percentile(fs, 66)
			return fmt.Sprintf("%f", f), nil
		},
	})
	cs = append(cs, &Column{
		Name: "p33",
		Func: func(fs []float64) (string, error) {
			f, _ := stats.Percentile(fs, 33)
			return fmt.Sprintf("%f", f), nil
		},
	})
	cs.AddPrefix("test_")
	cs = append(GetGroupColumns("fruits", "animals"), cs...)
	go NewWorker(5*time.Second).Start(rc, fc, cs)
	ec := make(chan error)
	go silkroad.NewLogger(",", "\n").LogRow(os.Stdout, rc, ec)
	go silkroad.LogError(os.Stderr, ec)
	for {
		fc <- NewSample(rand.Float64()+100, "banana", "dog")
		fc <- NewSample(100, "orange", "pig")
		fc <- NewSample(rand.Float64(), getRandomLabel1(), getRandomLabel2())
		time.Sleep(10 * time.Millisecond)
	}
}

func getRandomLabel1() string {
	return []string{"apple", "banana", "cherry"}[rand.Intn(3)]
}
func getRandomLabel2() string {
	return []string{"dog", "elephant", "flamingo"}[rand.Intn(3)]
}
