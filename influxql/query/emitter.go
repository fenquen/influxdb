package query

import (
	"github.com/influxdata/influxdb/v2/models"
)

// Emitter reads from a cursor into rows.
type Emitter struct {
	cursor    Cursor
	chunkSize int

	series  Series
	row     *models.Row
	columns []string
}

// NewEmitter returns a new instance of Emitter that pulls from itrs.
func NewEmitter(cur Cursor, chunkSize int) *Emitter {
	columns := make([]string, len(cur.Columns()))
	for i, col := range cur.Columns() {
		columns[i] = col.Val
	}
	return &Emitter{
		cursor:    cur,
		chunkSize: chunkSize,
		columns:   columns,
	}
}

// Close closes the underlying iterators.
func (emitter *Emitter) Close() error {
	return emitter.cursor.Close()
}

// returns the next row from the iterators.
func (emitter *Emitter) Emit() (*models.Row, bool, error) {
	// Continually read from the cursor until it is exhausted.
	for {
		// Scan the next row. If there are no rows left, return the current row.
		var row Row
		if !emitter.cursor.Scan(&row) {
			if err := emitter.cursor.Err(); err != nil {
				return nil, false, err
			}
			r := emitter.row
			emitter.row = nil
			return r, false, nil
		}

		// If there's no row yet then create one.
		// If the name and tags match the existing row, append to that row if
		// the number of values doesn't exceed the chunk size.
		// Otherwise return existing row and add values to next emitted row.
		if emitter.row == nil {
			emitter.createRow(row.Series, row.Values)
		} else if emitter.series.SameSeries(row.Series) {
			if emitter.chunkSize > 0 && len(emitter.row.Values) >= emitter.chunkSize {
				r := emitter.row
				r.Partial = true
				emitter.createRow(row.Series, row.Values)
				return r, true, nil
			}
			emitter.row.Values = append(emitter.row.Values, row.Values)
		} else {
			r := emitter.row
			emitter.createRow(row.Series, row.Values)
			return r, true, nil
		}
	}
}

// createRow creates a new row attached to the emitter.
func (emitter *Emitter) createRow(series Series, values []interface{}) {
	emitter.series = series
	emitter.row = &models.Row{
		Name:    series.Name,
		Tags:    series.Tags.KeyValues(),
		Columns: emitter.columns,
		Values:  [][]interface{}{values},
	}
}
