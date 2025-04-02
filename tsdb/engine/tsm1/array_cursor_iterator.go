package tsm1

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/metrics"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxql"
)

type arrayCursorIterator struct {
	engine *Engine
	key    []byte

	asc struct {
		Float    *floatArrayAscendingCursor
		Integer  *integerArrayAscendingCursor
		Unsigned *unsignedArrayAscendingCursor
		Boolean  *booleanArrayAscendingCursor
		String   *stringArrayAscendingCursor
	}

	desc struct {
		Float    *floatArrayDescendingCursor
		Integer  *integerArrayDescendingCursor
		Unsigned *unsignedArrayDescendingCursor
		Boolean  *booleanArrayDescendingCursor
		String   *stringArrayDescendingCursor
	}
}

func (arrayCursorIterator *arrayCursorIterator) Stats() tsdb.CursorStats {
	return tsdb.CursorStats{}
}

func (arrayCursorIterator *arrayCursorIterator) Next(ctx context.Context, r *tsdb.CursorRequest) (tsdb.Cursor, error) {
	// Look up fields for measurement.
	mf := arrayCursorIterator.engine.fieldset.Fields(r.Name)
	if mf == nil {
		return nil, nil
	}

	// Find individual field.
	f := mf.Field(r.Field)
	if f == nil {
		// field doesn't exist for this measurement
		return nil, nil
	}

	if grp := metrics.GroupFromContext(ctx); grp != nil {
		grp.GetCounter(numberOfRefCursorsCounter).Add(1)
	}

	var opt query.IteratorOptions
	opt.Ascending = r.Ascending
	opt.StartTime = r.StartTime
	opt.EndTime = r.EndTime // inclusive

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return arrayCursorIterator.buildFloatArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case influxql.Integer:
		return arrayCursorIterator.buildIntegerArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case influxql.Unsigned:
		return arrayCursorIterator.buildUnsignedArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case influxql.String:
		return arrayCursorIterator.buildStringArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case influxql.Boolean:
		return arrayCursorIterator.buildBooleanArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	default:
		panic(fmt.Sprintf("unreachable: %T", f.Type))
	}
}

func (arrayCursorIterator *arrayCursorIterator) seriesFieldKeyBytes(name []byte, tags models.Tags, field string) []byte {
	arrayCursorIterator.key = models.AppendMakeKey(arrayCursorIterator.key[:0], name, tags)
	arrayCursorIterator.key = append(arrayCursorIterator.key, keyFieldSeparatorBytes...)
	arrayCursorIterator.key = append(arrayCursorIterator.key, field...)
	return arrayCursorIterator.key
}
