package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/models"
)

// PointsWriter describes the ability to write points into a storage engine.
type PointsWriter interface { // 实际是 LoggingPointsWriter 证明在launcher.go:689
	WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error
}

// wraps an underlying points writer but writes logs to
// another bucket when an error occurs.
type LoggingPointsWriter struct {
	// Wrapped points writer, 实际是engine
	Underlying PointsWriter

	// Service used to look up logging bucket.
	BucketFinder BucketFinder

	// Name of the bucket to log to.
	LogBucketName string
}

// write points to the underlying PointsWriter
func (w *LoggingPointsWriter) WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, p []models.Point) error {
	if len(p) == 0 {
		return nil
	}

	// underlying实际是engine
	err := w.Underlying.WritePoints(ctx, orgID, bucketID, p)
	if err == nil {
		return nil
	}

	// 如果失败了写入log对应的bucket的
	bkts, n, e := w.BucketFinder.FindBuckets(ctx, influxdb.BucketFilter{
		OrganizationID: &orgID,
		Name:           &w.LogBucketName,
	})
	if e != nil {
		return e
	} else if n == 0 {
		return fmt.Errorf("logging bucket not found: %q", w.LogBucketName)
	}

	// Log error to bucket.
	pt, e := models.NewPoint(
		"write_errors",
		nil,
		models.Fields{"error": err.Error()},
		time.Now(),
	)
	if e != nil {
		return e
	}
	if e := w.Underlying.WritePoints(ctx, orgID, bkts[0].ID, []models.Point{pt}); e != nil {
		return e
	}

	return err
}
