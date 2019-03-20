package pinkpanther

import (
	"fmt"
	"time"

	"github.com/montanaflynn/stats"
)

type Column struct {
	Type ColumnType
	Name string
	Func func([]float64) (string, error)
}

type Columns []*Column

func (cs *Columns) AddPrefix(s string) {
	for i := 0; i < len(*cs); i++ {
		(*cs)[i].Name = s + (*cs)[i].Name
	}
}

func (cs *Columns) AddSuffix(s string) {
	for i := 0; i < len(*cs); i++ {
		(*cs)[i].Name = (*cs)[i].Name + s
	}
}

type ColumnType int

const (
	STATS ColumnType = iota
	GROUP
)

func GetDefaultColumns(groups ...string) Columns {
	return append(GetGroupColumns(groups...), GetDefaultStatColumns()...)
}

func GetDefaultColumnsWithLoggedAt(groups ...string) Columns {
	return append(GetLoggedAtColumn(), GetDefaultColumns(groups...)...)
}

func GetLoggedAtColumn() Columns {
	return Columns{
		{
			Name: "logged_at",
			Func: func(fs []float64) (string, error) {
				return time.Now().Format("15:04:05"), nil
			},
		},
	}
}

func GetGroupColumns(names ...string) Columns {
	var labelCols Columns
	for i := 0; i < len(names); i++ {
		labelCols = append(labelCols, &Column{
			Type: GROUP,
			Name: names[i],
		})
	}
	return labelCols
}

func GetDefaultStatColumns() Columns {
	// ignored errors because package 'stats' returns NaN as float64 whenever err != nil
	return Columns{
		{
			Name: "count",
			Func: func(fs []float64) (string, error) {
				return fmt.Sprintf("%d", len(fs)), nil
			},
		},
		{
			Name: "avg",
			Func: func(fs []float64) (string, error) {
				f, _ := stats.Mean(fs)
				return fmt.Sprintf("%f", f), nil
			},
		},
		{
			Name: "max",
			Func: func(fs []float64) (string, error) {
				f, _ := stats.Max(fs)
				return fmt.Sprintf("%f", f), nil
			},
		},
		{
			Name: "min",
			Func: func(fs []float64) (string, error) {
				f, _ := stats.Min(fs)
				return fmt.Sprintf("%f", f), nil
			},
		},
		{
			Name: "med",
			Func: func(fs []float64) (string, error) {
				f, _ := stats.Median(fs)
				return fmt.Sprintf("%f", f), nil
			},
		},
		{
			Name: "p90",
			Func: func(fs []float64) (string, error) {
				f, _ := stats.Percentile(fs, 90)
				return fmt.Sprintf("%f", f), nil
			},
		},
		{
			Name: "p95",
			Func: func(fs []float64) (string, error) {
				f, _ := stats.Percentile(fs, 95)
				return fmt.Sprintf("%f", f), nil
			},
		},
	}
}
