package metrics

import "github.com/streamingfast/dmetrics"

var MetricSet = dmetrics.NewSet()

var HeadBlockTimeDrift = MetricSet.NewHeadTimeDrift("merger")
var HeadBlockNumber = MetricSet.NewHeadBlockNumber("merger")
