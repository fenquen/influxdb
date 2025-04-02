package tsm1

import (
	"context"

	"github.com/influxdata/influxdb/v2/tsdb"
)

func (engine *Engine) CreateCursorIterator(ctx context.Context) (tsdb.CursorIterator, error) {
	return &arrayCursorIterator{engine: engine}, nil
}
