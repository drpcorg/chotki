package chotki

import (
	"fmt"

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

	// Cache metrics
	blockCacheSize   *prometheus.Desc
	blockCacheCount  *prometheus.Desc
	blockCacheHits   *prometheus.Desc
	blockCacheMisses *prometheus.Desc

	// Levels stats
	levelSublevels       *prometheus.Desc
	levelNumFiles        *prometheus.Desc
	levelSize            *prometheus.Desc
	levelScore           *prometheus.Desc
	levelBytesIn         *prometheus.Desc
	levelBytesIngested   *prometheus.Desc
	levelBytesMoved      *prometheus.Desc
	levelBytesRead       *prometheus.Desc
	levelBytesCompacted  *prometheus.Desc
	levelBytesFlushed    *prometheus.Desc
	levelTablesCompacted *prometheus.Desc
	levelTablesFlushed   *prometheus.Desc
	levelTablesIngested  *prometheus.Desc
	levelTablesMoved     *prometheus.Desc

	// Filters
	filterHits   *prometheus.Desc
	filterMisses *prometheus.Desc

	// Flush stats
	flushCount              *prometheus.Desc
	flushWriteThroughput    *prometheus.Desc
	flushNumInProgress      *prometheus.Desc
	flushAsIngestCount      *prometheus.Desc
	flushAsIngestTableCount *prometheus.Desc
	flushAsIngestBytes      *prometheus.Desc

	tableIters *prometheus.Desc

	// Keys
	rangeKeySetsCount *prometheus.Desc
	tombstoneCount    *prometheus.Desc

	// Snapshots
	snapshotCount          *prometheus.Desc
	snapshotEarliestSeqNum *prometheus.Desc
	snapshotPinnedKeys     *prometheus.Desc
	snapshotPinnedSize     *prometheus.Desc

	// Table
	tableObsoleteSize  *prometheus.Desc
	tableObsoleteCount *prometheus.Desc
	tableZombieSize    *prometheus.Desc
	tableZombieCount   *prometheus.Desc

	// TableCache
	tableCacheSize   *prometheus.Desc
	tableCacheCount  *prometheus.Desc
	tableCacheHits   *prometheus.Desc
	tableCacheMisses *prometheus.Desc
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
		blockCacheSize: prometheus.NewDesc(
			"pebble_block_cache_size_bytes",
			"Number of bytes used by the block cache",
			nil, nil,
		),
		blockCacheCount: prometheus.NewDesc(
			"pebble_block_cache_count",
			"Number of objects (blocks or tables) in the block cache",
			nil, nil,
		),
		blockCacheHits: prometheus.NewDesc(
			"pebble_block_cache_hits_total",
			"Number of block cache hits",
			nil, nil,
		),
		blockCacheMisses: prometheus.NewDesc(
			"pebble_block_cache_misses_total",
			"Number of block cache misses",
			nil, nil,
		),

		// Levels
		levelSublevels: prometheus.NewDesc(
			"pebble_level_sublevels",
			"Number of sublevels within each level",
			[]string{"level"}, nil,
		),
		levelNumFiles: prometheus.NewDesc(
			"pebble_level_num_files",
			"Total number of files in each level",
			[]string{"level"}, nil,
		),
		levelSize: prometheus.NewDesc(
			"pebble_level_size_bytes",
			"Total size of files in bytes in each level",
			[]string{"level"}, nil,
		),
		levelScore: prometheus.NewDesc(
			"pebble_level_score",
			"Compaction score for each level",
			[]string{"level"}, nil,
		),
		levelBytesIn: prometheus.NewDesc(
			"pebble_level_bytes_in",
			"Number of incoming bytes read into the level",
			[]string{"level"}, nil,
		),
		levelBytesIngested: prometheus.NewDesc(
			"pebble_level_bytes_ingested",
			"Number of bytes ingested into the level",
			[]string{"level"}, nil,
		),
		levelBytesMoved: prometheus.NewDesc(
			"pebble_level_bytes_moved",
			"Number of bytes moved into the level",
			[]string{"level"}, nil,
		),
		levelBytesRead: prometheus.NewDesc(
			"pebble_level_bytes_read",
			"Number of bytes read during compactions at this level",
			[]string{"level"}, nil,
		),
		levelBytesCompacted: prometheus.NewDesc(
			"pebble_level_bytes_compacted",
			"Number of bytes written during compactions",
			[]string{"level"}, nil,
		),
		levelBytesFlushed: prometheus.NewDesc(
			"pebble_level_bytes_flushed",
			"Number of bytes flushed to this level",
			[]string{"level"}, nil,
		),
		levelTablesCompacted: prometheus.NewDesc(
			"pebble_level_tables_compacted",
			"Number of SSTables compacted to this level",
			[]string{"level"}, nil,
		),
		levelTablesFlushed: prometheus.NewDesc(
			"pebble_level_tables_flushed",
			"Number of SSTables flushed to this level",
			[]string{"level"}, nil,
		),
		levelTablesIngested: prometheus.NewDesc(
			"pebble_level_tables_ingested",
			"Number of SSTables ingested into this level",
			[]string{"level"}, nil,
		),
		levelTablesMoved: prometheus.NewDesc(
			"pebble_level_tables_moved",
			"Number of SSTables moved to this level",
			[]string{"level"}, nil,
		),

		// Filters
		filterHits: prometheus.NewDesc(
			"pebble_filter_hits_total",
			"Number of filter policy hits that avoided accessing a data block",
			nil, nil,
		),
		filterMisses: prometheus.NewDesc(
			"pebble_filter_misses_total",
			"Number of filter policy misses that required accessing a data block",
			nil, nil,
		),

		// Flushes
		flushCount: prometheus.NewDesc(
			"pebble_flush_count_total",
			"Total number of flushes performed",
			nil, nil,
		),
		flushWriteThroughput: prometheus.NewDesc(
			"pebble_flush_write_throughput_bytes_per_second",
			"Write throughput during flushes in bytes per second",
			nil, nil,
		),
		flushNumInProgress: prometheus.NewDesc(
			"pebble_flush_in_progress_total",
			"Number of flushes currently in progress",
			nil, nil,
		),
		flushAsIngestCount: prometheus.NewDesc(
			"pebble_flush_as_ingest_count_total",
			"Number of flush operations handling ingested tables",
			nil, nil,
		),
		flushAsIngestTableCount: prometheus.NewDesc(
			"pebble_flush_as_ingest_table_count_total",
			"Number of tables ingested as flushables",
			nil, nil,
		),
		flushAsIngestBytes: prometheus.NewDesc(
			"pebble_flush_as_ingest_bytes_total",
			"Total bytes flushed for ingested flushables",
			nil, nil,
		),
		tableIters: prometheus.NewDesc(
			"pebble_table_iters_total",
			"Number of open table iterators",
			nil, nil,
		),
		// Keys
		rangeKeySetsCount: prometheus.NewDesc(
			"pebble_keys_range_key_sets_count",
			"Approximate count of internal range key set keys in the database",
			nil, nil,
		),
		tombstoneCount: prometheus.NewDesc(
			"pebble_keys_tombstone_count",
			"Approximate count of internal tombstones (DEL, SINGLEDEL, and RANGEDEL key kinds) in the database",
			nil, nil,
		),
		// Snapshots
		snapshotCount: prometheus.NewDesc(
			"pebble_snapshots_count",
			"Number of currently open snapshots",
			nil, nil,
		),
		snapshotEarliestSeqNum: prometheus.NewDesc(
			"pebble_snapshots_earliest_seq_num",
			"Sequence number of the earliest currently open snapshot",
			nil, nil,
		),
		snapshotPinnedKeys: prometheus.NewDesc(
			"pebble_snapshots_pinned_keys_total",
			"Running tally of keys pinned by open snapshots",
			nil, nil,
		),
		snapshotPinnedSize: prometheus.NewDesc(
			"pebble_snapshots_pinned_size_bytes",
			"Running cumulative sum of the size of keys and values pinned by open snapshots",
			nil, nil,
		),
		// Table
		tableObsoleteSize: prometheus.NewDesc(
			"pebble_table_obsolete_size_bytes",
			"Number of bytes in obsolete tables no longer referenced by the current DB state or iterators",
			nil, nil,
		),
		tableObsoleteCount: prometheus.NewDesc(
			"pebble_table_obsolete_count",
			"Number of obsolete tables no longer referenced by the current DB state or iterators",
			nil, nil,
		),
		tableZombieSize: prometheus.NewDesc(
			"pebble_table_zombie_size_bytes",
			"Number of bytes in zombie tables still in use by an iterator",
			nil, nil,
		),
		tableZombieCount: prometheus.NewDesc(
			"pebble_table_zombie_count",
			"Number of zombie tables still in use by an iterator",
			nil, nil,
		),

		// TableCache
		tableCacheSize: prometheus.NewDesc(
			"pebble_table_cache_size_bytes",
			"Number of bytes used by the table cache",
			nil, nil,
		),
		tableCacheCount: prometheus.NewDesc(
			"pebble_table_cache_count",
			"Number of objects in the table cache",
			nil, nil,
		),
		tableCacheHits: prometheus.NewDesc(
			"pebble_table_cache_hits_total",
			"Number of table cache hits",
			nil, nil,
		),
		tableCacheMisses: prometheus.NewDesc(
			"pebble_table_cache_misses_total",
			"Number of table cache misses",
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
	ch <- pc.blockCacheSize
	ch <- pc.blockCacheCount
	ch <- pc.blockCacheHits
	ch <- pc.blockCacheMisses

	// Levels
	ch <- pc.levelSublevels
	ch <- pc.levelNumFiles
	ch <- pc.levelSize
	ch <- pc.levelScore
	ch <- pc.levelBytesIn
	ch <- pc.levelBytesIngested
	ch <- pc.levelBytesMoved
	ch <- pc.levelBytesRead
	ch <- pc.levelBytesCompacted
	ch <- pc.levelBytesFlushed
	ch <- pc.levelTablesCompacted
	ch <- pc.levelTablesFlushed
	ch <- pc.levelTablesIngested
	ch <- pc.levelTablesMoved

	// Filters
	ch <- pc.filterHits
	ch <- pc.filterMisses

	// Flushes
	ch <- pc.flushCount
	ch <- pc.flushWriteThroughput
	ch <- pc.flushNumInProgress
	ch <- pc.flushAsIngestCount
	ch <- pc.flushAsIngestTableCount
	ch <- pc.flushAsIngestBytes

	ch <- pc.tableIters

	// Keys
	ch <- pc.rangeKeySetsCount
	ch <- pc.tombstoneCount

	// Snapshots
	ch <- pc.snapshotCount
	ch <- pc.snapshotEarliestSeqNum
	ch <- pc.snapshotPinnedKeys
	ch <- pc.snapshotPinnedSize

	// Table
	ch <- pc.tableObsoleteSize
	ch <- pc.tableObsoleteCount
	ch <- pc.tableZombieSize
	ch <- pc.tableZombieCount

	// TableCache
	ch <- pc.tableCacheSize
	ch <- pc.tableCacheCount
	ch <- pc.tableCacheHits
	ch <- pc.tableCacheMisses

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
	ch <- prometheus.MustNewConstMetric(
		pc.blockCacheSize,
		prometheus.GaugeValue,
		float64(metrics.BlockCache.Size),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.blockCacheCount,
		prometheus.GaugeValue,
		float64(metrics.BlockCache.Count),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.blockCacheHits,
		prometheus.CounterValue,
		float64(metrics.BlockCache.Hits),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.blockCacheMisses,
		prometheus.CounterValue,
		float64(metrics.BlockCache.Misses),
	)
	// In Collect
	for i, level := range metrics.Levels {
		levelLabel := fmt.Sprintf("%d", i)
		ch <- prometheus.MustNewConstMetric(
			pc.levelSublevels,
			prometheus.GaugeValue,
			float64(level.Sublevels),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelNumFiles,
			prometheus.GaugeValue,
			float64(level.NumFiles),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelSize,
			prometheus.GaugeValue,
			float64(level.Size),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelScore,
			prometheus.GaugeValue,
			level.Score,
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelBytesIn,
			prometheus.CounterValue,
			float64(level.BytesIn),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelBytesIngested,
			prometheus.CounterValue,
			float64(level.BytesIngested),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelBytesMoved,
			prometheus.CounterValue,
			float64(level.BytesMoved),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelBytesRead,
			prometheus.CounterValue,
			float64(level.BytesRead),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelBytesCompacted,
			prometheus.CounterValue,
			float64(level.BytesCompacted),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelBytesFlushed,
			prometheus.CounterValue,
			float64(level.BytesFlushed),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelTablesCompacted,
			prometheus.CounterValue,
			float64(level.TablesCompacted),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelTablesFlushed,
			prometheus.CounterValue,
			float64(level.TablesFlushed),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelTablesIngested,
			prometheus.CounterValue,
			float64(level.TablesIngested),
			levelLabel,
		)
		ch <- prometheus.MustNewConstMetric(
			pc.levelTablesMoved,
			prometheus.CounterValue,
			float64(level.TablesMoved),
			levelLabel,
		)
	}

	// Filters
	ch <- prometheus.MustNewConstMetric(
		pc.filterHits,
		prometheus.CounterValue,
		float64(metrics.Filter.Hits),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.filterMisses,
		prometheus.CounterValue,
		float64(metrics.Filter.Misses),
	)

	// Flushes
	ch <- prometheus.MustNewConstMetric(
		pc.flushCount,
		prometheus.CounterValue,
		float64(metrics.Flush.Count),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.flushWriteThroughput,
		prometheus.GaugeValue,
		float64(metrics.Flush.WriteThroughput.Rate()),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.flushNumInProgress,
		prometheus.GaugeValue,
		float64(metrics.Flush.NumInProgress),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.flushAsIngestCount,
		prometheus.CounterValue,
		float64(metrics.Flush.AsIngestCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.flushAsIngestTableCount,
		prometheus.CounterValue,
		float64(metrics.Flush.AsIngestTableCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.flushAsIngestBytes,
		prometheus.CounterValue,
		float64(metrics.Flush.AsIngestBytes),
	)

	ch <- prometheus.MustNewConstMetric(
		pc.tableIters,
		prometheus.GaugeValue,
		float64(metrics.TableIters),
	)

	// Keys
	ch <- prometheus.MustNewConstMetric(
		pc.rangeKeySetsCount,
		prometheus.GaugeValue,
		float64(metrics.Keys.RangeKeySetsCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.tombstoneCount,
		prometheus.GaugeValue,
		float64(metrics.Keys.TombstoneCount),
	)

	// Snapshots
	ch <- prometheus.MustNewConstMetric(
		pc.snapshotCount,
		prometheus.GaugeValue,
		float64(metrics.Snapshots.Count),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.snapshotEarliestSeqNum,
		prometheus.GaugeValue,
		float64(metrics.Snapshots.EarliestSeqNum),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.snapshotPinnedKeys,
		prometheus.CounterValue,
		float64(metrics.Snapshots.PinnedKeys),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.snapshotPinnedSize,
		prometheus.CounterValue,
		float64(metrics.Snapshots.PinnedSize),
	)

	// Table
	ch <- prometheus.MustNewConstMetric(
		pc.tableObsoleteSize,
		prometheus.GaugeValue,
		float64(metrics.Table.ObsoleteSize),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.tableObsoleteCount,
		prometheus.GaugeValue,
		float64(metrics.Table.ObsoleteCount),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.tableZombieSize,
		prometheus.GaugeValue,
		float64(metrics.Table.ZombieSize),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.tableZombieCount,
		prometheus.GaugeValue,
		float64(metrics.Table.ZombieCount),
	)

	// TableCache
	ch <- prometheus.MustNewConstMetric(
		pc.tableCacheSize,
		prometheus.GaugeValue,
		float64(metrics.TableCache.Size),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.tableCacheCount,
		prometheus.GaugeValue,
		float64(metrics.TableCache.Count),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.tableCacheHits,
		prometheus.CounterValue,
		float64(metrics.TableCache.Hits),
	)
	ch <- prometheus.MustNewConstMetric(
		pc.tableCacheMisses,
		prometheus.CounterValue,
		float64(metrics.TableCache.Misses),
	)

}
