package reads

import (
	"context"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type multiShardCursors interface {
	createCursor(row SeriesRow) cursors.Cursor
}

type resultSet struct {
	ctx               context.Context
	seriesCursor      SeriesCursor
	seriesRow         SeriesRow
	multiShardCursors multiShardCursors
}

// TODO(jsternberg): The range is [start, end) for this function which is consistent
// with the documented interface for datatypes.ReadFilterRequest. This function should
// be refactored to take in a datatypes.ReadFilterRequest similar to the other
// ResultSet functions.
func NewFilteredResultSet(ctx context.Context, start, end int64, seriesCursor SeriesCursor) ResultSet {
	return &resultSet{
		ctx:               ctx,
		seriesCursor:      seriesCursor,
		multiShardCursors: newMultiShardArrayCursors(ctx, start, end, true),
	}
}

func (resultSet *resultSet) Err() error { return nil }

// Close closes the result set. Close is idempotent.
func (resultSet *resultSet) Close() {
	if resultSet == nil {
		return // Nothing to do.
	}
	resultSet.seriesRow.CursorIterators = nil
	resultSet.seriesCursor.Close()
}

// Next returns true if there are more results available.
func (resultSet *resultSet) Next() bool {
	if resultSet == nil {
		return false
	}

	seriesRow := resultSet.seriesCursor.Next()
	if seriesRow == nil {
		return false
	}

	resultSet.seriesRow = *seriesRow

	return true
}

func (resultSet *resultSet) Cursor() cursors.Cursor {
	return resultSet.multiShardCursors.createCursor(resultSet.seriesRow)
}

func (resultSet *resultSet) Tags() models.Tags {
	return resultSet.seriesRow.Tags
}

// Stats returns the stats for the underlying cursors.
// Available after resultset has been scanned.
func (resultSet *resultSet) Stats() cursors.CursorStats {
	return resultSet.seriesRow.CursorIterators.Stats()
}
