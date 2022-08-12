package index_builder

import (
	"context"
	"fmt"

	"github.com/streamingfast/index-builder/metrics"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/firehose"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type IndexBuilder struct {
	*shutter.Shutter
	logger *zap.Logger

	startBlockNum uint64
	stopBlockNum  uint64

	handler bstream.Handler

	blocksStore dstore.Store
}

func NewIndexBuilder(logger *zap.Logger, handler bstream.Handler, startBlockNum, stopBlockNum uint64, blockStore dstore.Store) *IndexBuilder {
	return &IndexBuilder{
		Shutter:       shutter.New(),
		startBlockNum: startBlockNum,
		stopBlockNum:  stopBlockNum,
		handler:       handler,
		blocksStore:   blockStore,

		logger: logger,
	}
}

func (app *IndexBuilder) Launch() {
	err := app.launch()
	app.logger.Info("index builder exited", zap.Error(err))
	app.Shutdown(err)
}

func (app *IndexBuilder) launch() error {
	startBlockNum := app.startBlockNum
	stopBlockNum := app.stopBlockNum

	streamFactory := firehose.NewStreamFactory(
		app.blocksStore,
		nil,
		nil,
		nil,
	)
	ctx := context.Background()

	req := &pbfirehose.Request{
		StartBlockNum:   int64(startBlockNum),
		StopBlockNum:    stopBlockNum,
		FinalBlocksOnly: true,
	}

	handlerFunc := func(block *bstream.Block, obj interface{}) error {
		metrics.HeadBlockNumber.SetUint64(block.Number)
		metrics.HeadBlockTimeDrift.SetBlockTime(block.Time())
		metrics.AppReadiness.SetReady()

		return app.handler.ProcessBlock(block, obj)
	}

	stream, err := streamFactory.New(
		ctx,
		bstream.HandlerFunc(handlerFunc),
		req,
		true,
		app.logger,
	)

	if err != nil {
		return fmt.Errorf("getting firehose stream: %w", err)
	}

	return stream.Run(ctx)
}
