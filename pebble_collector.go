package chotki

import (
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
)

type PebbleCollector struct {
	db *pebble.DB

	// Prometheus descriptors for compaction metrics
	compactionCount         *prometheus.Desc
	compactionDefaultCount  *prometheus.Desc
	compactionElisionOnly   *prometheus.Desc
	compactionMove          *prometheus.Desc
	compactionRead          *prometheus.Desc
	compactionRewrite       *prometheus.Desc
	compactionMultiLevel    *prometheus.Desc
	compactionEstimatedDebt *prometheus.Desc
	compactionInProgress    *prometheus.Desc
	compactionMarkedFiles   *prometheus.Desc

	// Prometheus descriptors for memtable metrics
	memtableSize        *prometheus.Desc
	memtableCount       *prometheus.Desc
	memtableZombieSize  *prometheus.Desc
	memtableZombieCount *prometheus.Desc

	// Prometheus descriptors for WAL metrics
	walFiles         *prometheus.Desc
	walObsoleteFiles *prometheus.Desc
	walSize          *prometheus.Desc
	walBytesIn       *prometheus.Desc
	walBytesWritten  *prometheus.Desc
}

func NewPebbleCollector(db *pebble.DB) *PebbleCollector {
	return &PebbleCollector{
		db: db,

		// Compaction metrics
		compactionCount: prometheus.NewDesc(
			"pebble_compaction_count_total",
			"Total number of compactions performed",
			nil, nil,
		),
		compactionDefaultCount: prometheus.NewDesc(
			"pebble_compaction_default_count_total",
			"Total number of default compactions performed",
			nil, nil,
		),
		compactionElisionOnly: prometheus.NewDesc(
			"pebble_compaction_elision_only_total",
			"Total number of elision-only compactions performed",
			nil, nil,
		),
		compactionMove: prometheus.NewDesc(
			"pebble_compaction_move_total",
			"Total number of move compactions performed",
			nil, nil,
		),
		compactionRead: prometheus.NewDesc(
			"pebble_compaction_read_total",
			"Total number of read compactions performed",
			nil, nil,
		),
		compactionRewrite: prometheus.NewDesc(
			"pebble_compaction_rewrite_total",
			"Total number of rewrite compactions performed",
			nil, nil,
		),
		compactionMultiLevel: prometheus.NewDesc(
			"pebble_compaction_multilevel_total",
			"Total number of multi-level compactions performed",
			nil, nil,
		),
		compactionEstimatedDebt: prometheus.NewDesc(
			"pebble_compaction_estimated_debt_bytes",
			"Estimated number of bytes that need to be compacted to reach a stable state",
			nil, nil,
		),
		compactionInProgress: prometheus.NewDesc(
			"pebble_compaction_in_progress_bytes",
			"Number of bytes being compacted currently",
			nil, nil,
		),
		compactionMarkedFiles: prometheus.NewDesc(
			"pebble_compaction_marked_files_total",
			"Number of files marked for compaction",
			nil, nil,
		),

		// Memtable metrics
		memtableSize: prometheus.NewDesc(
			"pebble_memtable_size_bytes",
			"Current size of the memtable in bytes",
			nil, nil,
		),
		memtableCount: prometheus.NewDesc(
			"pebble_memtable_count_total",
			"Current count of memtables",
			nil, nil,
		),
		memtableZombieSize: prometheus.NewDesc(
			"pebble_memtable_zombie_size_bytes",
			"Size of zombie memtables in bytes",
			nil, nil,
		),
		memtableZombieCount: prometheus.NewDesc(
			"pebble_memtable_zombie_count_total",
			"Count of zombie memtables",
			nil, nil,
		),

		// WAL metrics
		walFiles: prometheus.NewDesc(
			"pebble_wal_files_total",
			"Number of live WAL files",
			nil, nil,
		),
		walObsoleteFiles: prometheus.NewDesc(
			"pebble_wal_obsolete_files_total",
			"Number of obsolete WAL files",
			nil, nil,
		),
		walSize: prometheus.NewDesc(
			"pebble_wal_size_bytes",
			"Size of live WAL data in bytes",
			nil, nil,
		),
		walBytesIn: prometheus.NewDesc(
			"pebble_wal_bytes_in_total",
			"Total logical bytes written to the WAL",
			nil, nil,
		),
		walBytesWritten: prometheus.NewDesc(
			"pebble_wal_bytes_written_total",
			"Total physical bytes written to the WAL",
			nil, nil,
		),
	}
}

func (pc *PebbleCollector) Describe(ch chan<- *prometheus.Desc) {
	// Compaction metrics
	ch <- pc.compactionCount
	ch <- pc.compactionDefaultCount
	ch <- pc.compactionElisionOnly
	ch <- pc.compactionMove
	ch <- pc.compactionRead
	ch <- pc.compactionRewrite
	ch <- pc.compactionMultiLevel
	ch <- pc.compactionEstimatedDebt
	ch <- pc.compactionInProgress
	ch <- pc.compactionMarkedFiles

	// Memtable metrics
	ch <- pc.memtableSize
	ch <- pc.memtableCount
	ch <- pc.memtableZombieSize
	ch <- pc.memtableZombieCount

	// WAL metrics
	ch <- pc.walFiles
	ch <- pc.walObsoleteFiles
	ch <- pc.walSize
	ch <- pc.walBytesIn
	ch <- pc.walBytesWritten
}

func (pc *PebbleCollector) Collect(ch chan<- prometheus.Metric) {
	metrics := pc.db.Metrics()

	// Compaction metrics
	ch <- prometheus.MustNewConstMetric(
		pc.compactionCount,
		prometheus.CounterValue,
		float64(metrics.Compact.Count),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionDefaultCount,
		prometheus.CounterValue,
		float64(metrics.Compact.DefaultCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionElisionOnly,
		prometheus.CounterValue,
		float64(metrics.Compact.ElisionOnlyCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionMove,
		prometheus.CounterValue,
		float64(metrics.Compact.MoveCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionRead,
		prometheus.CounterValue,
		float64(metrics.Compact.ReadCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionRewrite,
		prometheus.CounterValue,
		float64(metrics.Compact.RewriteCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionMultiLevel,
		prometheus.CounterValue,
		float64(metrics.Compact.MultiLevelCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionEstimatedDebt,
		prometheus.GaugeValue,
		float64(metrics.Compact.EstimatedDebt),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionInProgress,
		prometheus.GaugeValue,
		float64(metrics.Compact.InProgressBytes),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.compactionMarkedFiles,
		prometheus.GaugeValue,
		float64(metrics.Compact.MarkedFiles),
	)

	// Memtable metrics
	ch <- prometheus.MustNewConstMetric(
		pc.memtableSize,
		prometheus.GaugeValue,
		float64(metrics.MemTable.Size),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.memtableCount,
		prometheus.GaugeValue,
		float64(metrics.MemTable.Count),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.memtableZombieSize,
		prometheus.GaugeValue,
		float64(metrics.MemTable.ZombieSize),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.memtableZombieCount,
		prometheus.GaugeValue,
		float64(metrics.MemTable.ZombieCount),
	)

	// WAL metrics
	ch <- prometheus.MustNewConstMetric(
		pc.walFiles,
		prometheus.GaugeValue,
		float64(metrics.WAL.Files),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.walObsoleteFiles,
		prometheus.GaugeValue,
		float64(metrics.WAL.ObsoleteFiles),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.walSize,
		prometheus.GaugeValue,
		float64(metrics.WAL.Size),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.walBytesIn,
		prometheus.CounterValue,
		float64(metrics.WAL.BytesIn),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.walBytesWritten,
		prometheus.CounterValue,
		float64(metrics.WAL.BytesWritten),
	)
}
