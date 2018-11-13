package pinkpanther

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/yuichiro12/silkroad"
)

func TestBatch(t *testing.T) {
	rc := make(chan []string)
	ec := make(chan error)
	fc := make(chan Sample)
	cs := GetDefaultColumnsWithLoggedAt()
	go NewWorker(10*time.Second, ec).Start(cs, fc, rc)
	go silkroad.NewLogger(os.Stdout, ec, ",").LogRow(rc)
	go silkroad.LogError(os.Stderr, nil)
	for {
		fc <- NewSample(rand.Float64())
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBatchWithOptions(t *testing.T) {
	rc := make(chan []string)
	fc := make(chan Sample)
	ec := make(chan error)
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
	go NewWorker(5*time.Second, ec).Start(cs, fc, rc)
	go silkroad.NewLogger(os.Stdout, ec, ",").LogRow(rc)
	go silkroad.LogError(os.Stderr, ec)
	rc <- GetHeader(cs)
	for {
		fc <- NewSample(rand.Float64()+100, "banana", "dog")
		fc <- NewSample(100, "orange", "pig")
		fc <- NewSample(rand.Float64(), getRandomLabel1(), getRandomLabel2())
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStop(t *testing.T) {
	ec := make(chan error)
	w := NewWorker(1*time.Second, ec)
	spl := make(chan Sample)
	s := make(chan []string)
	for i := 0; i < 100; i++ {
		go w.Start(GetDefaultColumnsWithLoggedAt(), spl, s)
	}
	go func() {
		for {
			spl <- NewSample(0.1)
		}
	}()
	go func() {
		for {
			_ = <-s
		}
	}()
	time.Sleep(3 * time.Second)
	w.Stop()
}

func getRandomLabel1() string {
	return []string{"apple", "banana", "cherry"}[rand.Intn(3)]
}
func getRandomLabel2() string {
	return []string{"dog", "elephant", "flamingo"}[rand.Intn(3)]
}
