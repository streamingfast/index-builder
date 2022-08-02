package index_builder

import (
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/dstore"
	index_builder "github.com/streamingfast/index-builder"
	"github.com/streamingfast/index-builder/metrics"
	"github.com/streamingfast/shutter"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

type Config struct {
	BlockHandler   bstream.Handler
	StartBlock     uint64
	EndBlock       uint64
	BlockStorePath string
}

type App struct {
	*shutter.Shutter
	config         *Config
	readinessProbe pbhealth.HealthClient
}

func New(config *Config) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
	}
}

func (a *App) Run() error {
	blockStore, err := dstore.NewStore(a.config.BlockStorePath, "", "", false)
	if err != nil {
		return err
	}

	indexBuilder := index_builder.NewIndexBuilder(
		zlog,
		a.config.BlockHandler,
		a.config.StartBlock,
		a.config.EndBlock,
		blockStore,
	)

	dmetrics.Register(metrics.MetricSet)

	a.OnTerminating(indexBuilder.Shutdown)
	indexBuilder.OnTerminated(a.Shutdown)

	go indexBuilder.Launch()

	zlog.Info("index builder running")
	return nil
}
