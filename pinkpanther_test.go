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
	go NewWorker(10*time.Second).Start(cs, fc, rc)
	go silkroad.NewLogger(",").LogRow(os.Stdout, rc, ec)
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
	go NewWorker(5*time.Second).Start(cs, fc, rc)
	ec := make(chan error)
	go silkroad.NewLogger(",").LogRow(os.Stdout, rc, ec)
	go silkroad.LogError(os.Stderr, ec)
	for {
		fc <- NewSample(rand.Float64()+100, "banana", "dog")
		fc <- NewSample(100, "orange", "pig")
		fc <- NewSample(rand.Float64(), getRandomLabel1(), getRandomLabel2())
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStop(t *testing.T) {
	w := NewWorker(1 * time.Second)
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
