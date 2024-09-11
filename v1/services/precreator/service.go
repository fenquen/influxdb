// Package precreator provides the shard precreation service.
package precreator // import "github.com/influxdata/influxdb/v2/v1/services/precreator"

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/logger"
	"go.uber.org/zap"
)

// Service manages the shard precreation service.
type Service struct {
	checkInterval time.Duration
	advancePeriod time.Duration // 对应 storage-shard-precreator-advance-period

	Logger *zap.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup

	MetaClient interface {
		PrecreateShardGroups(now, cutoff time.Time) error
	}
}

// NewService returns an instance of the precreation service.
func NewService(c Config) *Service {
	return &Service{
		checkInterval: time.Duration(c.CheckInterval),
		advancePeriod: time.Duration(c.AdvancePeriod),
		Logger:        zap.NewNop(),
	}
}

// WithLogger sets the logger for the service.
func (service *Service) WithLogger(log *zap.Logger) {
	service.Logger = log.With(zap.String("service", "shard-precreation"))
}

// Open starts the precreation service.
func (service *Service) Open(ctx context.Context) error {
	if service.cancel != nil {
		return nil
	}

	service.Logger.Info("Starting precreation service",
		logger.DurationLiteral("check_interval", service.checkInterval),
		logger.DurationLiteral("advance_period", service.advancePeriod))

	ctx, service.cancel = context.WithCancel(ctx)

	service.wg.Add(1)
	go service.runPrecreation(ctx)
	return nil
}

// Close stops the precreation service.
func (service *Service) Close() error {
	if service.cancel == nil {
		return nil
	}

	service.cancel()
	service.wg.Wait()
	service.cancel = nil

	return nil
}

// runPrecreation continually checks if resources need precreation.
func (service *Service) runPrecreation(ctx context.Context) {
	defer service.wg.Done()

	for {
		select {
		case <-time.After(service.checkInterval):
			if err := service.precreate(time.Now().UTC()); err != nil {
				service.Logger.Info("Failed to precreate shards", zap.Error(err))
			}
		case <-ctx.Done():
			service.Logger.Info("Terminating precreation service")
			return
		}
	}
}

// performs actual resource precreation.
func (service *Service) precreate(now time.Time) error {
	cutoff := now.Add(service.advancePeriod).UTC()
	return service.MetaClient.PrecreateShardGroups(now, cutoff)
}
