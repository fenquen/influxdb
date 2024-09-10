package launcher

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/testing"
	"github.com/influxdata/flux/dependencies/url"
	"github.com/influxdata/flux/execute/executetest"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/annotations"
	annotationTransport "github.com/influxdata/influxdb/v2/annotations/transport"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/backup"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/checks"
	"github.com/influxdata/influxdb/v2/dashboards"
	dashboardTransport "github.com/influxdata/influxdb/v2/dashboards/transport"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/gather"
	"github.com/influxdata/influxdb/v2/http"
	iqlcontrol "github.com/influxdata/influxdb/v2/influxql/control"
	iqlquery "github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/internal/resource"
	"github.com/influxdata/influxdb/v2/kit/feature"
	overrideflagger "github.com/influxdata/influxdb/v2/kit/feature/override"
	"github.com/influxdata/influxdb/v2/kit/metric"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/notebooks"
	notebookTransport "github.com/influxdata/influxdb/v2/notebooks/transport"
	endpointservice "github.com/influxdata/influxdb/v2/notification/endpoint/service"
	ruleservice "github.com/influxdata/influxdb/v2/notification/rule/service"
	"github.com/influxdata/influxdb/v2/pkger"
	infprom "github.com/influxdata/influxdb/v2/prometheus"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/control"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/remotes"
	remotesTransport "github.com/influxdata/influxdb/v2/remotes/transport"
	"github.com/influxdata/influxdb/v2/replications"
	replicationTransport "github.com/influxdata/influxdb/v2/replications/transport"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/session"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/source"
	"github.com/influxdata/influxdb/v2/sqlite"
	sqliteMigrations "github.com/influxdata/influxdb/v2/sqlite/migrations"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/storage/readservice"
	taskbackend "github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/backend/coordinator"
	"github.com/influxdata/influxdb/v2/task/backend/executor"
	"github.com/influxdata/influxdb/v2/task/backend/middleware"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	telegrafservice "github.com/influxdata/influxdb/v2/telegraf/service"
	"github.com/influxdata/influxdb/v2/telemetry"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/prometheus/client_golang/prometheus/collectors"

	// needed for tsm1
	_ "github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"

	// needed for tsi1
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	authv1 "github.com/influxdata/influxdb/v2/v1/authorization"
	iqlcoordinator "github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	storage2 "github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/influxdata/influxdb/v2/vault"
	pzap "github.com/influxdata/influxdb/v2/zap"
	"github.com/opentracing/opentracing-go"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
)

const (
	// DiskStore stores all REST resources to disk in boltdb and sqlite.
	DiskStore = "disk"
	// BoltStore also stores all REST resources to disk in boltdb and sqlite. Kept for backwards-compatibility.
	BoltStore = "bolt"
	// MemoryStore stores all REST resources in memory (useful for testing).
	MemoryStore = "memory"

	// LogTracing enables tracing via zap logs
	LogTracing = "log"
	// JaegerTracing enables tracing via the Jaeger client library
	JaegerTracing = "jaeger"
)

type labeledCloser struct {
	label  string
	closer func(context.Context) error
}

// represents the main program execution.
type Launcher struct {
	wg       sync.WaitGroup
	cancel   func()
	doneChan <-chan struct{}
	closers  []labeledCloser
	flushers flushers

	flagger feature.Flagger

	kvStore   kv.Store
	kvService *kv.Service
	sqlStore  *sqlite.SqlStore

	// storage engine
	engine Engine

	// InfluxQL query engine
	queryController *control.Controller

	httpPort   int
	tlsEnabled bool

	scheduler stoppingScheduler
	executor  *executor.Executor

	log *zap.Logger
	reg *prom.Registry

	apibackend *http.APIBackend
}

type stoppingScheduler interface {
	scheduler.Scheduler
	Stop()
}

// NewLauncher returns a new instance of Launcher with a no-op logger.
func NewLauncher() *Launcher {
	return &Launcher{
		log: zap.NewNop(),
	}
}

// Registry returns the prometheus metrics registry.
func (launcher *Launcher) Registry() *prom.Registry {
	return launcher.reg
}

// Engine returns a reference to the storage engine. It should only be called
// for end-to-end testing purposes.
func (launcher *Launcher) Engine() Engine {
	return launcher.engine
}

// Shutdown shuts down the HTTP server and waits for all services to clean up.
func (launcher *Launcher) Shutdown(ctx context.Context) error {
	var errs []string

	// Shut down subsystems in the reverse order of their registration.
	for i := len(launcher.closers); i > 0; i-- {
		lc := launcher.closers[i-1]
		launcher.log.Info("Stopping subsystem", zap.String("subsystem", lc.label))
		if err := lc.closer(ctx); err != nil {
			launcher.log.Error("Failed to stop subsystem", zap.String("subsystem", lc.label), zap.Error(err))
			errs = append(errs, err.Error())
		}
	}

	launcher.wg.Wait()

	// N.B. We ignore any errors here because Sync is known to fail with EINVAL
	// when logging to Stdout on certain OS's.
	//
	// Uber made the same change within the core of the logger implementation.
	// See: https://github.com/uber-go/zap/issues/328
	_ = launcher.log.Sync()

	if len(errs) > 0 {
		return fmt.Errorf("failed to shut down server: [%s]", strings.Join(errs, ","))
	}
	return nil
}

func (launcher *Launcher) Done() <-chan struct{} {
	return launcher.doneChan
}

func (launcher *Launcher) run(ctx context.Context, opts *InfluxdOpts) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	ctx, launcher.cancel = context.WithCancel(ctx)
	launcher.doneChan = ctx.Done()

	info := platform.GetBuildInfo()
	launcher.log.Info("Welcome to InfluxDB",
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("build_date", info.Date),
		zap.String("log_level", opts.LogLevel.String()),
	)
	launcher.initTracing(opts)

	if p := opts.Viper.ConfigFileUsed(); p != "" {
		launcher.log.Debug("loaded config file", zap.String("path", p))
	}

	if opts.NatsPort != 0 {
		launcher.log.Warn("nats-port argument is deprecated and unused")
	}

	if opts.NatsMaxPayloadBytes != 0 {
		launcher.log.Warn("nats-max-payload-bytes argument is deprecated and unused")
	}

	// Parse feature flags.
	// These flags can be used to modify the remaining setup logic in this method.
	// They will also be injected into the contexts of incoming HTTP requests at runtime,
	// for use in modifying behavior there.
	if launcher.flagger == nil {
		launcher.flagger = feature.DefaultFlagger()
		if len(opts.FeatureFlags) > 0 {
			f, err := overrideflagger.Make(opts.FeatureFlags, feature.ByKey)
			if err != nil {
				launcher.log.Error("Failed to configure feature flag overrides",
					zap.Error(err), zap.Any("overrides", opts.FeatureFlags))
				return err
			}
			launcher.log.Info("Running with feature flag overrides", zap.Any("overrides", opts.FeatureFlags))
			launcher.flagger = f
		}
	}

	launcher.reg = prom.NewRegistry(launcher.log.With(zap.String("service", "prom_registry")))
	launcher.reg.MustRegister(collectors.NewGoCollector())

	// Open KV and SQL stores.
	procID, err := launcher.openMetaStores(ctx, opts)
	if err != nil {
		return err
	}
	launcher.reg.MustRegister(infprom.NewInfluxCollector(procID, info))

	tenantStore := tenant.NewStore(launcher.kvStore)
	ts := tenant.NewSystem(tenantStore, launcher.log.With(zap.String("store", "new")), launcher.reg, metric.WithSuffix("new"))

	serviceConfig := kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	}

	launcher.kvService = kv.NewService(launcher.log.With(zap.String("store", "kv")), launcher.kvStore, ts, serviceConfig)

	var (
		opLogSvc                                              = tenant.NewOpLogService(launcher.kvStore, launcher.kvService)
		userLogSvc   platform.UserOperationLogService         = opLogSvc
		bucketLogSvc platform.BucketOperationLogService       = opLogSvc
		orgLogSvc    platform.OrganizationOperationLogService = opLogSvc
	)
	var (
		variableSvc      platform.VariableService           = launcher.kvService
		sourceSvc        platform.SourceService             = launcher.kvService
		scraperTargetSvc platform.ScraperTargetStoreService = launcher.kvService
	)

	var authSvc platform.AuthorizationService
	{
		authStore, err := authorization.NewStore(launcher.kvStore)
		if err != nil {
			launcher.log.Error("Failed creating new authorization store", zap.Error(err))
			return err
		}
		authSvc = authorization.NewService(authStore, ts)
	}

	secretStore, err := secret.NewStore(launcher.kvStore)
	if err != nil {
		launcher.log.Error("Failed creating new secret store", zap.Error(err))
		return err
	}

	var secretSvc platform.SecretService = secret.NewMetricService(launcher.reg, secret.NewLogger(launcher.log.With(zap.String("service", "secret")), secret.NewService(secretStore)))

	switch opts.SecretStore {
	case "bolt":
		// If it is bolt, then we already set it above.
	case "vault":
		// The vault secret service is configured using the standard vault environment variables.
		// https://www.vaultproject.io/docs/commands/index.html#environment-variables
		svc, err := vault.NewSecretService(vault.WithConfig(opts.VaultConfig))
		if err != nil {
			launcher.log.Error("Failed initializing vault secret service", zap.Error(err))
			return err
		}
		secretSvc = svc
	default:
		err := fmt.Errorf("unknown secret service %q, expected \"bolt\" or \"vault\"", opts.SecretStore)
		launcher.log.Error("Failed setting secret service", zap.Error(err))
		return err
	}

	metaClient := meta.NewClient(meta.NewConfig(), launcher.kvStore)
	if err := metaClient.Open(); err != nil {
		launcher.log.Error("Failed to open meta client", zap.Error(err))
		return err
	}

	if opts.Testing {
		// the testing engine will write/read into a temporary directory
		engine := NewTemporaryEngine(
			opts.StorageConfig,
			storage.WithMetaClient(metaClient),
		)
		launcher.flushers = append(launcher.flushers, engine)
		launcher.engine = engine
	} else {
		// check for 2.x data / state from a prior 2.x
		if err := checkForPriorVersion(ctx, launcher.log, opts.BoltPath, opts.EnginePath, ts.BucketService, metaClient); err != nil {
			os.Exit(1)
		}

		launcher.engine = storage.NewEngine(
			opts.EnginePath,
			opts.StorageConfig,
			storage.WithMetricsDisabled(opts.MetricsDisabled),
			storage.WithMetaClient(metaClient),
		)
	}
	launcher.engine.WithLogger(launcher.log)
	if err := launcher.engine.Open(ctx); err != nil {
		launcher.log.Error("Failed to open engine", zap.Error(err))
		return err
	}
	launcher.closers = append(launcher.closers, labeledCloser{
		label: "engine",
		closer: func(context.Context) error {
			return launcher.engine.Close()
		},
	})
	// The Engine's metrics must be registered after it opens.
	launcher.reg.MustRegister(launcher.engine.PrometheusCollectors()...)

	var (
		deleteService  platform.DeleteService  = launcher.engine
		pointsWriter   storage.PointsWriter    = launcher.engine
		backupService  platform.BackupService  = launcher.engine
		restoreService platform.RestoreService = launcher.engine
	)

	remotesSvc := remotes.NewService(launcher.sqlStore)
	remotesServer := remotesTransport.NewInstrumentedRemotesHandler(
		launcher.log.With(zap.String("handler", "remotes")), launcher.reg, launcher.kvStore, remotesSvc)

	replicationSvc, replicationsMetrics := replications.NewService(launcher.sqlStore, ts, pointsWriter, launcher.log.With(zap.String("service", "replications")), opts.EnginePath, opts.InstanceID)
	replicationServer := replicationTransport.NewInstrumentedReplicationHandler(
		launcher.log.With(zap.String("handler", "replications")), launcher.reg, launcher.kvStore, replicationSvc)
	ts.BucketService = replications.NewBucketService(
		launcher.log.With(zap.String("service", "replication_buckets")), ts.BucketService, replicationSvc)

	launcher.reg.MustRegister(replicationsMetrics.PrometheusCollectors()...)

	if err = replicationSvc.Open(ctx); err != nil {
		launcher.log.Error("Failed to open replications service", zap.Error(err))
		return err
	}

	launcher.closers = append(launcher.closers, labeledCloser{
		label: "replications",
		closer: func(context.Context) error {
			return replicationSvc.Close()
		},
	})

	pointsWriter = replicationSvc

	// When --hardening-enabled, use an HTTP IP validator that restricts
	// flux and pkger HTTP requests to private addressess.
	var urlValidator url.Validator
	if opts.HardeningEnabled {
		urlValidator = url.PrivateIPValidator{}
	} else {
		urlValidator = url.PassValidator{}
	}

	deps, err := influxdb.NewDependencies(
		storageflux.NewReader(storage2.NewStore(launcher.engine.TSDBStore(), launcher.engine.MetaClient())),
		pointsWriter,
		authorizer.NewBucketService(ts.BucketService),
		authorizer.NewOrgService(ts.OrganizationService),
		authorizer.NewSecretService(secretSvc),
		nil,
		influxdb.WithURLValidator(urlValidator),
	)
	if err != nil {
		launcher.log.Error("Failed to get query controller dependencies", zap.Error(err))
		return err
	}

	dependencyList := []flux.Dependency{deps}
	if opts.Testing {
		dependencyList = append(dependencyList, executetest.NewDefaultTestFlagger())
		dependencyList = append(dependencyList, testing.FrameworkConfig{})
	}

	launcher.queryController, err = control.New(control.Config{
		ConcurrencyQuota:                opts.ConcurrencyQuota,
		InitialMemoryBytesQuotaPerQuery: opts.InitialMemoryBytesQuotaPerQuery,
		MemoryBytesQuotaPerQuery:        opts.MemoryBytesQuotaPerQuery,
		MaxMemoryBytes:                  opts.MaxMemoryBytes,
		QueueSize:                       opts.QueueSize,
		ExecutorDependencies:            dependencyList,
		FluxLogEnabled:                  opts.FluxLogEnabled,
	}, launcher.log.With(zap.String("service", "storage-reads")))
	if err != nil {
		launcher.log.Error("Failed to create query controller", zap.Error(err))
		return err
	}
	launcher.closers = append(launcher.closers, labeledCloser{
		label: "query",
		closer: func(ctx context.Context) error {
			return launcher.queryController.Shutdown(ctx)
		},
	})

	launcher.reg.MustRegister(launcher.queryController.PrometheusCollectors()...)

	var storageQueryService = readservice.NewProxyQueryService(launcher.queryController)
	var taskSvc taskmodel.TaskService
	{
		// create the task stack
		combinedTaskService := taskbackend.NewAnalyticalStorage(
			launcher.log.With(zap.String("service", "task-analytical-store")),
			launcher.kvService,
			ts.BucketService,
			launcher.kvService,
			pointsWriter,
			query.QueryServiceBridge{AsyncQueryService: launcher.queryController},
		)

		executor, executorMetrics := executor.NewExecutor(
			launcher.log.With(zap.String("service", "task-executor")),
			query.QueryServiceBridge{AsyncQueryService: launcher.queryController},
			ts.UserService,
			combinedTaskService,
			combinedTaskService,
			executor.WithFlagger(launcher.flagger),
		)
		err = executor.LoadExistingScheduleRuns(ctx)
		if err != nil {
			launcher.log.Fatal("could not load existing scheduled runs", zap.Error(err))
		}
		launcher.executor = executor
		launcher.reg.MustRegister(executorMetrics.PrometheusCollectors()...)
		schLogger := launcher.log.With(zap.String("service", "task-scheduler"))

		var sch stoppingScheduler = &scheduler.NoopScheduler{}
		if !opts.NoTasks {
			var (
				sm  *scheduler.SchedulerMetrics
				err error
			)
			sch, sm, err = scheduler.NewScheduler(
				executor,
				taskbackend.NewSchedulableTaskService(launcher.kvService),
				scheduler.WithOnErrorFn(func(ctx context.Context, taskID scheduler.ID, scheduledAt time.Time, err error) {
					schLogger.Info(
						"error in scheduler run",
						zap.String("taskID", platform2.ID(taskID).String()),
						zap.Time("scheduledAt", scheduledAt),
						zap.Error(err))
				}),
			)
			if err != nil {
				launcher.log.Fatal("could not start task scheduler", zap.Error(err))
			}
			launcher.closers = append(launcher.closers, labeledCloser{
				label: "task",
				closer: func(context.Context) error {
					sch.Stop()
					return nil
				},
			})
			launcher.reg.MustRegister(sm.PrometheusCollectors()...)
		}

		launcher.scheduler = sch

		coordLogger := launcher.log.With(zap.String("service", "task-coordinator"))
		taskCoord := coordinator.NewCoordinator(
			coordLogger,
			sch,
			executor)

		taskSvc = middleware.New(combinedTaskService, taskCoord)
		if err := taskbackend.TaskNotifyCoordinatorOfExisting(
			ctx,
			taskSvc,
			combinedTaskService,
			taskCoord,
			func(ctx context.Context, taskID platform2.ID, runID platform2.ID) error {
				_, err := executor.ResumeCurrentRun(ctx, taskID, runID)
				return err
			},
			coordLogger); err != nil {
			launcher.log.Error("Failed to resume existing tasks", zap.Error(err))
		}
	}

	dbrpSvc := dbrp.NewAuthorizedService(dbrp.NewService(ctx, authorizer.NewBucketService(ts.BucketService), launcher.kvStore))

	cm := iqlcontrol.NewControllerMetrics([]string{})
	launcher.reg.MustRegister(cm.PrometheusCollectors()...)

	mapper := &iqlcoordinator.LocalShardMapper{
		MetaClient: metaClient,
		TSDBStore:  launcher.engine.TSDBStore(),
		DBRP:       dbrpSvc,
	}

	launcher.log.Info("Configuring InfluxQL statement executor (zeros indicate unlimited).",
		zap.Int("max_select_point", opts.CoordinatorConfig.MaxSelectPointN),
		zap.Int("max_select_series", opts.CoordinatorConfig.MaxSelectSeriesN),
		zap.Int("max_select_buckets", opts.CoordinatorConfig.MaxSelectBucketsN))

	qe := iqlquery.NewExecutor(launcher.log, cm)
	se := &iqlcoordinator.StatementExecutor{
		MetaClient:        metaClient,
		TSDBStore:         launcher.engine.TSDBStore(),
		ShardMapper:       mapper,
		DBRP:              dbrpSvc,
		MaxSelectPointN:   opts.CoordinatorConfig.MaxSelectPointN,
		MaxSelectSeriesN:  opts.CoordinatorConfig.MaxSelectSeriesN,
		MaxSelectBucketsN: opts.CoordinatorConfig.MaxSelectBucketsN,
	}
	qe.StatementExecutor = se
	qe.StatementNormalizer = se

	var checkSvc platform.CheckService
	{
		coordinator := coordinator.NewCoordinator(launcher.log, launcher.scheduler, launcher.executor)
		checkSvc = checks.NewService(launcher.log.With(zap.String("svc", "checks")), launcher.kvStore, ts.OrganizationService, launcher.kvService)
		checkSvc = middleware.NewCheckService(checkSvc, launcher.kvService, coordinator)
	}

	var notificationEndpointSvc platform.NotificationEndpointService
	{
		notificationEndpointSvc = endpointservice.New(endpointservice.NewStore(launcher.kvStore), secretSvc)
	}

	var notificationRuleSvc platform.NotificationRuleStore
	{
		coordinator := coordinator.NewCoordinator(launcher.log, launcher.scheduler, launcher.executor)
		notificationRuleSvc, err = ruleservice.New(launcher.log, launcher.kvStore, launcher.kvService, ts.OrganizationService, notificationEndpointSvc)
		if err != nil {
			return err
		}

		// tasks service notification middleware which keeps task service up to date
		// with persisted changes to notification rules.
		notificationRuleSvc = middleware.NewNotificationRuleStore(notificationRuleSvc, launcher.kvService, coordinator)
	}

	var telegrafSvc platform.TelegrafConfigStore
	{
		telegrafSvc = telegrafservice.New(launcher.kvStore)
	}

	scraperScheduler, err := gather.NewScheduler(launcher.log.With(zap.String("service", "scraper")), 100, 10, scraperTargetSvc, pointsWriter, 10*time.Second)
	if err != nil {
		launcher.log.Error("Failed to create scraper subscriber", zap.Error(err))
		return err
	}
	launcher.closers = append(launcher.closers, labeledCloser{
		label: "scraper",
		closer: func(ctx context.Context) error {
			scraperScheduler.Close()
			return nil
		},
	})

	var sessionSvc platform.SessionService
	{
		sessionSvc = session.NewService(
			session.NewStorage(inmem.NewSessionStore()),
			ts.UserService,
			ts.UserResourceMappingService,
			authSvc,
			session.WithSessionLength(time.Duration(opts.SessionLength)*time.Minute),
		)
		sessionSvc = session.NewSessionMetrics(launcher.reg, sessionSvc)
		sessionSvc = session.NewSessionLogger(launcher.log.With(zap.String("service", "session")), sessionSvc)
	}

	var labelSvc platform.LabelService
	{
		labelsStore, err := label.NewStore(launcher.kvStore)
		if err != nil {
			launcher.log.Error("Failed creating new labels store", zap.Error(err))
			return err
		}
		labelSvc = label.NewService(labelsStore)
	}

	ts.BucketService = storage.NewBucketService(launcher.log, ts.BucketService, launcher.engine)
	ts.BucketService = dbrp.NewBucketService(launcher.log, ts.BucketService, dbrpSvc)

	bucketManifestWriter := backup.NewBucketManifestWriter(ts, metaClient)

	onboardingLogger := launcher.log.With(zap.String("handler", "onboard"))
	onboardOpts := []tenant.OnboardServiceOptionFn{tenant.WithOnboardingLogger(onboardingLogger)}
	if opts.TestingAlwaysAllowSetup {
		onboardOpts = append(onboardOpts, tenant.WithAlwaysAllowInitialUser())
	}

	onboardSvc := tenant.NewOnboardService(ts, authSvc, onboardOpts...)                          // basic service
	onboardSvc = tenant.NewAuthedOnboardSvc(onboardSvc)                                          // with auth
	onboardSvc = tenant.NewOnboardingMetrics(launcher.reg, onboardSvc, metric.WithSuffix("new")) // with metrics
	onboardSvc = tenant.NewOnboardingLogger(onboardingLogger, onboardSvc)                        // with logging

	var (
		passwordV1 platform.PasswordsService
		authSvcV1  *authv1.Service
	)
	{
		authStore, err := authv1.NewStore(launcher.kvStore)
		if err != nil {
			launcher.log.Error("Failed creating new authorization store", zap.Error(err))
			return err
		}

		authSvcV1 = authv1.NewService(authStore, ts)
		passwordV1 = authv1.NewCachingPasswordsService(authSvcV1)
	}

	var (
		dashboardSvc    platform.DashboardService
		dashboardLogSvc platform.DashboardOperationLogService
	)
	{
		dashboardService := dashboards.NewService(launcher.kvStore, launcher.kvService)
		dashboardSvc = dashboardService
		dashboardLogSvc = dashboardService
	}

	// resourceResolver is a deprecated type which combines the lookups
	// of multiple resources into one type, used to resolve the resources
	// associated org ID or name . It is a stop-gap while we move this
	// behaviour off of *kv.Service to aid in reducing the coupling on this type.
	resourceResolver := &resource.Resolver{
		AuthorizationFinder:        authSvc,
		BucketFinder:               ts.BucketService,
		OrganizationFinder:         ts.OrganizationService,
		DashboardFinder:            dashboardSvc,
		SourceFinder:               sourceSvc,
		TaskFinder:                 taskSvc,
		TelegrafConfigFinder:       telegrafSvc,
		VariableFinder:             variableSvc,
		TargetFinder:               scraperTargetSvc,
		CheckFinder:                checkSvc,
		NotificationEndpointFinder: notificationEndpointSvc,
		NotificationRuleFinder:     notificationRuleSvc,
	}

	errorHandler := kithttp.NewErrorHandler(launcher.log.With(zap.String("handler", "error_logger")))
	launcher.apibackend = &http.APIBackend{
		AssetsPath:           opts.AssetsPath,
		UIDisabled:           opts.UIDisabled,
		HTTPErrorHandler:     errorHandler,
		Logger:               launcher.log,
		FluxLogEnabled:       opts.FluxLogEnabled,
		SessionRenewDisabled: opts.SessionRenewDisabled,
		NewQueryService:      source.NewQueryService,
		PointsWriter: &storage.LoggingPointsWriter{
			Underlying:    pointsWriter,
			BucketFinder:  ts.BucketService,
			LogBucketName: platform.MonitoringSystemBucketName,
		},
		DeleteService:           deleteService,
		BackupService:           backupService,
		SqlBackupRestoreService: launcher.sqlStore,
		BucketManifestWriter:    bucketManifestWriter,
		RestoreService:          restoreService,
		AuthorizationService:    authSvc,
		AuthorizationV1Service:  authSvcV1,
		PasswordV1Service:       passwordV1,
		AuthorizerV1: &authv1.Authorizer{
			AuthV1:   authSvcV1,
			AuthV2:   authSvc,
			Comparer: passwordV1,
			User:     ts,
		},
		AlgoWProxy: &http.NoopProxyHandler{},
		// Wrap the BucketService in a storage backed one that will ensure deleted buckets are removed from the storage engine.
		BucketService:                   ts.BucketService,
		SessionService:                  sessionSvc,
		UserService:                     ts.UserService,
		OnboardingService:               onboardSvc,
		DBRPService:                     dbrpSvc,
		OrganizationService:             ts.OrganizationService,
		UserResourceMappingService:      ts.UserResourceMappingService,
		LabelService:                    labelSvc,
		DashboardService:                dashboardSvc,
		DashboardOperationLogService:    dashboardLogSvc,
		BucketOperationLogService:       bucketLogSvc,
		UserOperationLogService:         userLogSvc,
		OrganizationOperationLogService: orgLogSvc,
		SourceService:                   sourceSvc,
		VariableService:                 variableSvc,
		PasswordsService:                ts.PasswordsService,
		InfluxqldService:                iqlquery.NewProxyExecutor(launcher.log, qe),
		FluxService:                     storageQueryService,
		FluxLanguageService:             fluxlang.DefaultService,
		TaskService:                     taskSvc,
		TelegrafService:                 telegrafSvc,
		NotificationRuleStore:           notificationRuleSvc,
		NotificationEndpointService:     notificationEndpointSvc,
		CheckService:                    checkSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		SecretService:                   secretSvc,
		LookupService:                   resourceResolver,
		DocumentService:                 launcher.kvService,
		OrgLookupService:                resourceResolver,
		WriteEventRecorder:              infprom.NewEventRecorder("write"),
		QueryEventRecorder:              infprom.NewEventRecorder("query"),
		Flagger:                         launcher.flagger,
		FlagsHandler:                    feature.NewFlagsHandler(errorHandler, feature.ByKey),
	}

	launcher.reg.MustRegister(launcher.apibackend.PrometheusCollectors()...)

	authAgent := new(authorizer.AuthAgent)

	var pkgSVC pkger.SVC
	{
		b := launcher.apibackend
		authedOrgSVC := authorizer.NewOrgService(b.OrganizationService)
		authedUrmSVC := authorizer.NewURMService(b.OrgLookupService, b.UserResourceMappingService)
		pkgerLogger := launcher.log.With(zap.String("service", "pkger"))
		pkgSVC = pkger.NewService(
			pkger.WithHTTPClient(pkger.NewDefaultHTTPClient(urlValidator)),
			pkger.WithLogger(pkgerLogger),
			pkger.WithStore(pkger.NewStoreKV(launcher.kvStore)),
			pkger.WithBucketSVC(authorizer.NewBucketService(b.BucketService)),
			pkger.WithCheckSVC(authorizer.NewCheckService(b.CheckService, authedUrmSVC, authedOrgSVC)),
			pkger.WithDashboardSVC(authorizer.NewDashboardService(b.DashboardService)),
			pkger.WithLabelSVC(label.NewAuthedLabelService(labelSvc, b.OrgLookupService)),
			pkger.WithNotificationEndpointSVC(authorizer.NewNotificationEndpointService(b.NotificationEndpointService, authedUrmSVC, authedOrgSVC)),
			pkger.WithNotificationRuleSVC(authorizer.NewNotificationRuleStore(b.NotificationRuleStore, authedUrmSVC, authedOrgSVC)),
			pkger.WithOrganizationService(authorizer.NewOrgService(b.OrganizationService)),
			pkger.WithSecretSVC(authorizer.NewSecretService(b.SecretService)),
			pkger.WithTaskSVC(authorizer.NewTaskService(pkgerLogger, b.TaskService)),
			pkger.WithTelegrafSVC(authorizer.NewTelegrafConfigService(b.TelegrafService, b.UserResourceMappingService)),
			pkger.WithVariableSVC(authorizer.NewVariableService(b.VariableService)),
		)
		pkgSVC = pkger.MWTracing()(pkgSVC)
		pkgSVC = pkger.MWMetrics(launcher.reg)(pkgSVC)
		pkgSVC = pkger.MWLogging(pkgerLogger)(pkgSVC)
		pkgSVC = pkger.MWAuth(authAgent)(pkgSVC)
	}

	var stacksHTTPServer *pkger.HTTPServerStacks
	{
		tLogger := launcher.log.With(zap.String("handler", "stacks"))
		stacksHTTPServer = pkger.NewHTTPServerStacks(tLogger, pkgSVC)
	}

	var templatesHTTPServer *pkger.HTTPServerTemplates
	{
		tLogger := launcher.log.With(zap.String("handler", "templates"))
		templatesHTTPServer = pkger.NewHTTPServerTemplates(tLogger, pkgSVC, pkger.NewDefaultHTTPClient(urlValidator))
	}

	userHTTPServer := ts.NewUserHTTPHandler(launcher.log)
	meHTTPServer := ts.NewMeHTTPHandler(launcher.log)
	onboardHTTPServer := tenant.NewHTTPOnboardHandler(launcher.log, onboardSvc)

	// feature flagging for new labels service
	var labelHandler *label.LabelHandler
	{
		b := launcher.apibackend

		labelSvc = label.NewAuthedLabelService(labelSvc, b.OrgLookupService)
		labelSvc = label.NewLabelLogger(launcher.log.With(zap.String("handler", "labels")), labelSvc)
		labelSvc = label.NewLabelMetrics(launcher.reg, labelSvc)
		labelHandler = label.NewHTTPLabelHandler(launcher.log, labelSvc)
	}

	// feature flagging for new authorization service
	var authHTTPServer *authorization.AuthHandler
	{
		authLogger := launcher.log.With(zap.String("handler", "authorization"))

		var authService platform.AuthorizationService
		authService = authorization.NewAuthedAuthorizationService(authSvc, ts)
		authService = authorization.NewAuthMetrics(launcher.reg, authService)
		authService = authorization.NewAuthLogger(authLogger, authService)

		authHTTPServer = authorization.NewHTTPAuthHandler(launcher.log, authService, ts)
	}

	var v1AuthHTTPServer *authv1.AuthHandler
	{
		authLogger := launcher.log.With(zap.String("handler", "v1_authorization"))

		var authService platform.AuthorizationService
		authService = authorization.NewAuthedAuthorizationService(authSvcV1, ts)
		authService = authorization.NewAuthLogger(authLogger, authService)

		passService := authv1.NewAuthedPasswordService(authv1.AuthFinder(authSvcV1), passwordV1)
		v1AuthHTTPServer = authv1.NewHTTPAuthHandler(launcher.log, authService, passService, ts)
	}

	var sessionHTTPServer *session.SessionHandler
	{
		sessionHTTPServer = session.NewSessionHandler(launcher.log.With(zap.String("handler", "session")), sessionSvc, ts.UserService, ts.PasswordsService)
	}

	orgHTTPServer := ts.NewOrgHTTPHandler(launcher.log, secret.NewAuthedService(secretSvc))

	bucketHTTPServer := ts.NewBucketHTTPHandler(launcher.log, labelSvc)

	var dashboardServer *dashboardTransport.DashboardHandler
	{
		urmHandler := tenant.NewURMHandler(
			launcher.log.With(zap.String("handler", "urm")),
			platform.DashboardsResourceType,
			"id",
			ts.UserService,
			tenant.NewAuthedURMService(ts.OrganizationService, ts.UserResourceMappingService),
		)

		labelHandler := label.NewHTTPEmbeddedHandler(
			launcher.log.With(zap.String("handler", "label")),
			platform.DashboardsResourceType,
			labelSvc,
		)

		dashboardServer = dashboardTransport.NewDashboardHandler(
			launcher.log.With(zap.String("handler", "dashboards")),
			authorizer.NewDashboardService(dashboardSvc),
			labelSvc,
			ts.UserService,
			ts.OrganizationService,
			urmHandler,
			labelHandler,
		)
	}

	notebookSvc := notebooks.NewService(launcher.sqlStore)
	notebookServer := notebookTransport.NewNotebookHandler(
		launcher.log.With(zap.String("handler", "notebooks")),
		authorizer.NewNotebookService(
			notebooks.NewLoggingService(
				launcher.log.With(zap.String("service", "notebooks")),
				notebooks.NewMetricCollectingService(launcher.reg, notebookSvc),
			),
		),
	)

	annotationSvc := annotations.NewService(launcher.sqlStore)
	annotationServer := annotationTransport.NewAnnotationHandler(
		launcher.log.With(zap.String("handler", "annotations")),
		authorizer.NewAnnotationService(
			annotations.NewLoggingService(
				launcher.log.With(zap.String("service", "annotations")),
				annotations.NewMetricCollectingService(launcher.reg, annotationSvc),
			),
		),
	)

	configHandler, err := http.NewConfigHandler(launcher.log.With(zap.String("handler", "config")), opts.BindCliOpts())
	if err != nil {
		return err
	}

	platformHandler := http.NewPlatformHandler(
		launcher.apibackend,
		http.WithResourceHandler(stacksHTTPServer),
		http.WithResourceHandler(templatesHTTPServer),
		http.WithResourceHandler(onboardHTTPServer),
		http.WithResourceHandler(authHTTPServer),
		http.WithResourceHandler(labelHandler),
		http.WithResourceHandler(sessionHTTPServer.SignInResourceHandler()),
		http.WithResourceHandler(sessionHTTPServer.SignOutResourceHandler()),
		http.WithResourceHandler(userHTTPServer),
		http.WithResourceHandler(meHTTPServer),
		http.WithResourceHandler(orgHTTPServer),
		http.WithResourceHandler(bucketHTTPServer),
		http.WithResourceHandler(v1AuthHTTPServer),
		http.WithResourceHandler(dashboardServer),
		http.WithResourceHandler(notebookServer),
		http.WithResourceHandler(annotationServer),
		http.WithResourceHandler(remotesServer),
		http.WithResourceHandler(replicationServer),
		http.WithResourceHandler(configHandler),
	)

	httpLogger := launcher.log.With(zap.String("service", "http"))
	var httpHandler nethttp.Handler = http.NewRootHandler(
		"platform",
		http.WithLog(httpLogger),
		http.WithAPIHandler(platformHandler),
		http.WithPprofEnabled(!opts.ProfilingDisabled),
		http.WithMetrics(launcher.reg, !opts.MetricsDisabled),
	)

	if opts.LogLevel == zap.DebugLevel {
		httpHandler = http.LoggingMW(httpLogger)(httpHandler)
	}
	// If we are in testing mode we allow all data to be flushed and removed.
	if opts.Testing {
		httpHandler = http.Debug(ctx, httpHandler, launcher.flushers, onboardSvc)
	}

	if !opts.ReportingDisabled {
		launcher.runReporter(ctx)
	}
	if err := launcher.runHTTP(opts, httpHandler, httpLogger); err != nil {
		return err
	}

	return nil
}

// initTracing sets up the global tracer for the influxd process.
// Any errors encountered during setup are logged, but don't crash the process.
func (launcher *Launcher) initTracing(opts *InfluxdOpts) {
	switch opts.TracingType {
	case LogTracing:
		launcher.log.Info("Tracing via zap logging")
		opentracing.SetGlobalTracer(pzap.NewTracer(launcher.log, snowflake.NewIDGenerator()))

	case JaegerTracing:
		launcher.log.Info("Tracing via Jaeger")
		cfg, err := jaegerconfig.FromEnv()
		if err != nil {
			launcher.log.Error("Failed to get Jaeger client config from environment variables", zap.Error(err))
			return
		}
		tracer, closer, err := cfg.NewTracer()
		if err != nil {
			launcher.log.Error("Failed to instantiate Jaeger tracer", zap.Error(err))
			return
		}
		launcher.closers = append(launcher.closers, labeledCloser{
			label: "Jaeger tracer",
			closer: func(context.Context) error {
				return closer.Close()
			},
		})
		opentracing.SetGlobalTracer(tracer)
	}
}

// openMetaStores opens the embedded DBs used to store metadata about influxd resources, migrating them to
// the latest schema expected by the server.
// On success, a unique ID is returned to be used as an identifier for the influxd instance in telemetry.
func (launcher *Launcher) openMetaStores(ctx context.Context, opts *InfluxdOpts) (string, error) {
	type flushableKVStore interface {
		kv.SchemaStore
		http.Flusher
	}
	var kvStore flushableKVStore
	var sqlStore *sqlite.SqlStore

	var procID string
	var err error
	switch opts.StoreType {
	case BoltStore:
		launcher.log.Warn("Using --store=bolt is deprecated. Use --store=disk instead.")
		fallthrough
	case DiskStore:
		boltClient := bolt.NewClient(launcher.log.With(zap.String("service", "bolt")))
		boltClient.Path = opts.BoltPath

		if err := boltClient.Open(ctx); err != nil {
			launcher.log.Error("Failed opening bolt", zap.Error(err))
			return "", err
		}
		launcher.closers = append(launcher.closers, labeledCloser{
			label: "bolt",
			closer: func(context.Context) error {
				return boltClient.Close()
			},
		})
		launcher.reg.MustRegister(boltClient)
		procID = boltClient.ID().String()

		boltKV := bolt.NewKVStore(launcher.log.With(zap.String("service", "kvstore-bolt")), opts.BoltPath)
		boltKV.WithDB(boltClient.DB())
		kvStore = boltKV

		// If a sqlite-path is not specified, store sqlite db in the same directory as bolt with the default filename.
		if opts.SqLitePath == "" {
			opts.SqLitePath = filepath.Join(filepath.Dir(opts.BoltPath), sqlite.DefaultFilename)
		}
		sqlStore, err = sqlite.NewSqlStore(opts.SqLitePath, launcher.log.With(zap.String("service", "sqlite")))
		if err != nil {
			launcher.log.Error("Failed opening sqlite store", zap.Error(err))
			return "", err
		}

	case MemoryStore:
		kvStore = inmem.NewKVStore()
		sqlStore, err = sqlite.NewSqlStore(sqlite.InmemPath, launcher.log.With(zap.String("service", "sqlite")))
		if err != nil {
			launcher.log.Error("Failed opening sqlite store", zap.Error(err))
			return "", err
		}

	default:
		err := fmt.Errorf("unknown store type %s; expected disk or memory", opts.StoreType)
		launcher.log.Error("Failed opening metadata store", zap.Error(err))
		return "", err
	}

	launcher.closers = append(launcher.closers, labeledCloser{
		label: "sqlite",
		closer: func(context.Context) error {
			return sqlStore.Close()
		},
	})
	if opts.Testing {
		launcher.flushers = append(launcher.flushers, kvStore, sqlStore)
	}

	// Apply migrations to the KV and SQL metadata stores.
	kvMigrator, err := migration.NewMigrator(
		launcher.log.With(zap.String("service", "KV migrations")),
		kvStore,
		all.Migrations[:]...,
	)
	if err != nil {
		launcher.log.Error("Failed to initialize kv migrator", zap.Error(err))
		return "", err
	}
	sqlMigrator := sqlite.NewMigrator(sqlStore, launcher.log.With(zap.String("service", "SQL migrations")))

	// If we're migrating a persistent data store, take a backup of the pre-migration state for rollback.
	if opts.StoreType == DiskStore || opts.StoreType == BoltStore {
		backupPattern := "%s.pre-%s-upgrade.backup"
		info := platform.GetBuildInfo()
		kvMigrator.SetBackupPath(fmt.Sprintf(backupPattern, opts.BoltPath, info.Version))
		sqlMigrator.SetBackupPath(fmt.Sprintf(backupPattern, opts.SqLitePath, info.Version))
	}
	if err := kvMigrator.Up(ctx); err != nil {
		launcher.log.Error("Failed to apply KV migrations", zap.Error(err))
		return "", err
	}
	if err := sqlMigrator.Up(ctx, sqliteMigrations.AllUp); err != nil {
		launcher.log.Error("Failed to apply SQL migrations", zap.Error(err))
		return "", err
	}

	launcher.kvStore = kvStore
	launcher.sqlStore = sqlStore
	return procID, nil
}

// runHTTP configures and launches a listener for incoming HTTP(S) requests.
// The listener is run in a separate goroutine. If it fails to start up, it
// will cancel the launcher.
func (launcher *Launcher) runHTTP(opts *InfluxdOpts, handler nethttp.Handler, httpLogger *zap.Logger) error {
	log := launcher.log.With(zap.String("service", "tcp-listener"))

	httpServer := &nethttp.Server{
		Addr:              opts.HttpBindAddress,
		Handler:           handler,
		ReadHeaderTimeout: opts.HttpReadHeaderTimeout,
		ReadTimeout:       opts.HttpReadTimeout,
		WriteTimeout:      opts.HttpWriteTimeout,
		IdleTimeout:       opts.HttpIdleTimeout,
		ErrorLog:          zap.NewStdLog(httpLogger),
	}
	launcher.closers = append(launcher.closers, labeledCloser{
		label:  "HTTP server",
		closer: httpServer.Shutdown,
	})

	ln, err := net.Listen("tcp", opts.HttpBindAddress)
	if err != nil {
		log.Error("Failed to set up TCP listener", zap.String("addr", opts.HttpBindAddress), zap.Error(err))
		return err
	}
	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		launcher.httpPort = addr.Port
	}
	launcher.wg.Add(1)

	launcher.tlsEnabled = opts.HttpTLSCert != "" && opts.HttpTLSKey != ""
	if !launcher.tlsEnabled {
		if opts.HttpTLSCert != "" || opts.HttpTLSKey != "" {
			log.Warn("TLS requires specifying both cert and key, falling back to HTTP")
		}

		go func(log *zap.Logger) {
			defer launcher.wg.Done()
			log.Info("Listening", zap.String("transport", "http"), zap.String("addr", opts.HttpBindAddress), zap.Int("port", launcher.httpPort))

			if err := httpServer.Serve(ln); err != nethttp.ErrServerClosed {
				log.Error("Failed to serve HTTP", zap.Error(err))
				launcher.cancel()
			}
			log.Info("Stopping")
		}(log)

		return nil
	}

	if _, err = tls.LoadX509KeyPair(opts.HttpTLSCert, opts.HttpTLSKey); err != nil {
		log.Error("Failed to load x509 key pair", zap.String("cert-path", opts.HttpTLSCert), zap.String("key-path", opts.HttpTLSKey))
		return err
	}

	var tlsMinVersion uint16
	var useStrictCiphers = opts.HttpTLSStrictCiphers
	switch opts.HttpTLSMinVersion {
	case "1.0":
		log.Warn("Setting the minimum version of TLS to 1.0 - this is discouraged. Please use 1.2 or 1.3")
		tlsMinVersion = tls.VersionTLS10
	case "1.1":
		log.Warn("Setting the minimum version of TLS to 1.1 - this is discouraged. Please use 1.2 or 1.3")
		tlsMinVersion = tls.VersionTLS11
	case "1.2":
		tlsMinVersion = tls.VersionTLS12
	case "1.3":
		if useStrictCiphers {
			log.Warn("TLS version 1.3 does not support configuring strict ciphers")
			useStrictCiphers = false
		}
		tlsMinVersion = tls.VersionTLS13
	default:
		return fmt.Errorf("unsupported TLS version: %s", opts.HttpTLSMinVersion)
	}

	// nil uses the default cipher suite
	var cipherConfig []uint16 = nil
	if useStrictCiphers {
		// See https://ssl-config.mozilla.org/#server=go&version=1.14.4&config=intermediate&guideline=5.6
		cipherConfig = []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		}
	}

	httpServer.TLSConfig = &tls.Config{
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: !useStrictCiphers,
		MinVersion:               tlsMinVersion,
		CipherSuites:             cipherConfig,
	}

	go func(log *zap.Logger) {
		defer launcher.wg.Done()
		log.Info("Listening", zap.String("transport", "https"), zap.String("addr", opts.HttpBindAddress), zap.Int("port", launcher.httpPort))

		if err := httpServer.ServeTLS(ln, opts.HttpTLSCert, opts.HttpTLSKey); err != nethttp.ErrServerClosed {
			log.Error("Failed to serve HTTPS", zap.Error(err))
			launcher.cancel()
		}
		log.Info("Stopping")
	}(log)

	return nil
}

// runReporter configures and launches a periodic telemetry report for the server.
func (launcher *Launcher) runReporter(ctx context.Context) {
	reporter := telemetry.NewReporter(launcher.log, launcher.reg)
	reporter.Interval = 8 * time.Hour
	launcher.wg.Add(1)
	go func() {
		defer launcher.wg.Done()
		reporter.Report(ctx)
	}()
}

func checkForPriorVersion(ctx context.Context, log *zap.Logger, boltPath string, enginePath string, bs platform.BucketService, metaClient *meta.Client) error {
	buckets, _, err := bs.FindBuckets(ctx, platform.BucketFilter{})
	if err != nil {
		log.Error("Failed to retrieve buckets", zap.Error(err))
		return err
	}

	hasErrors := false

	// if there are no buckets, we will be fine
	if len(buckets) > 0 {
		log.Info("Checking InfluxDB metadata for prior version.", zap.String("bolt_path", boltPath))

		for i := range buckets {
			bucket := buckets[i]
			if dbi := metaClient.Database(bucket.ID.String()); dbi == nil {
				log.Error("Missing metadata for bucket.", zap.String("bucket", bucket.Name), zap.Stringer("bucket_id", bucket.ID))
				hasErrors = true
			}
		}

		if hasErrors {
			log.Error("Incompatible InfluxDB 2.0 metadata found. File must be moved before influxd will start.", zap.String("path", boltPath))
		}
	}

	// see if there are existing files which match the old directory structure
	{
		for _, name := range []string{"_series", "index"} {
			dir := filepath.Join(enginePath, name)
			if fi, err := os.Stat(dir); err == nil {
				if fi.IsDir() {
					log.Error("Found directory that is incompatible with this version of InfluxDB.", zap.String("path", dir))
					hasErrors = true
				}
			}
		}
	}

	if hasErrors {
		log.Error("Incompatible InfluxDB 2.0 version found. Move all files outside of engine_path before influxd will start.", zap.String("engine_path", enginePath))
		return errors.New("incompatible InfluxDB version")
	}

	return nil
}

// OrganizationService returns the internal organization service.
func (launcher *Launcher) OrganizationService() platform.OrganizationService {
	return launcher.apibackend.OrganizationService
}

// QueryController returns the internal query service.
func (launcher *Launcher) QueryController() *control.Controller {
	return launcher.queryController
}

// BucketService returns the internal bucket service.
func (launcher *Launcher) BucketService() platform.BucketService {
	return launcher.apibackend.BucketService
}

// UserService returns the internal user service.
func (launcher *Launcher) UserService() platform.UserService {
	return launcher.apibackend.UserService
}

// AuthorizationService returns the internal authorization service.
func (launcher *Launcher) AuthorizationService() platform.AuthorizationService {
	return launcher.apibackend.AuthorizationService
}

func (launcher *Launcher) AuthorizationV1Service() platform.AuthorizationService {
	return launcher.apibackend.AuthorizationV1Service
}

// SecretService returns the internal secret service.
func (launcher *Launcher) SecretService() platform.SecretService {
	return launcher.apibackend.SecretService
}

// CheckService returns the internal check service.
func (launcher *Launcher) CheckService() platform.CheckService {
	return launcher.apibackend.CheckService
}

func (launcher *Launcher) DBRPMappingService() platform.DBRPMappingService {
	return launcher.apibackend.DBRPService
}

func (launcher *Launcher) SessionService() platform.SessionService {
	return launcher.apibackend.SessionService
}
