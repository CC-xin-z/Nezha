package kvstore

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Read-path counters add visible contention at high GET concurrency. Keep the
// instrumentation in the tree for diagnosis, but compile the hot-path calls out
// of performance builds.
const bphsReadStatsEnabled = false

// bphsWriteStats separates BPHS's extra durability work from the common Raft
// path. The key values are dirty partitions per apply batch and the resulting
// number of partition fsync rounds.
var bphsWriteStats struct {
	puts                atomic.Uint64
	putBytes            atomic.Uint64
	putLockWaitNs       atomic.Uint64
	syncBatches         atomic.Uint64
	dirtyPartitions     atomic.Uint64
	syncRounds          atomic.Uint64
	partitionLockNs     atomic.Uint64
	recordSyncBatches   atomic.Uint64
	recordSyncRequested atomic.Uint64
	recordSyncDeduped   atomic.Uint64
	recordSyncCalls     atomic.Uint64
	recordSyncNs        atomic.Uint64
	vlogSyncCalls       atomic.Uint64
	vlogSyncNs          atomic.Uint64
	walSyncCalls        atomic.Uint64
	walSyncNs           atomic.Uint64
	walBytes            atomic.Uint64
	walResets           atomic.Uint64
	syncTotalNs         atomic.Uint64
	memtableFlushes     atomic.Uint64
	memtableFlushNs     atomic.Uint64
	memtableFlushB      atomic.Uint64
	l0Merges            atomic.Uint64
	l0MergeNs           atomic.Uint64
	baseCompactions     atomic.Uint64
	baseCompactionNs    atomic.Uint64
	bloomProbes         atomic.Uint64
	bloomSkips          atomic.Uint64
	memHits             atomic.Uint64
	l0Hits              atomic.Uint64
	baseHits            atomic.Uint64
	readMisses          atomic.Uint64
	scanQueries         atomic.Uint64
	scanPartitions      atomic.Uint64
	scanL0Runs          atomic.Uint64
}

func recordBPHSPut(valueBytes int, lockWait time.Duration) {
	bphsWriteStats.puts.Add(1)
	bphsWriteStats.putBytes.Add(uint64(valueBytes))
	bphsWriteStats.putLockWaitNs.Add(uint64(lockWait))
}

func recordBPHSSyncBatch(dirty, concurrency int, elapsed time.Duration) {
	if dirty <= 0 {
		return
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	rounds := (dirty + concurrency - 1) / concurrency
	bphsWriteStats.syncBatches.Add(1)
	bphsWriteStats.dirtyPartitions.Add(uint64(dirty))
	bphsWriteStats.syncRounds.Add(uint64(rounds))
	bphsWriteStats.syncTotalNs.Add(uint64(elapsed))
}

func recordBPHSPartitionLockWait(elapsed time.Duration) {
	bphsWriteStats.partitionLockNs.Add(uint64(elapsed))
}

func recordBPHSRecordSync(elapsed time.Duration) {
	bphsWriteStats.recordSyncCalls.Add(1)
	bphsWriteStats.recordSyncNs.Add(uint64(elapsed))
}

func recordBPHSRecordSyncBatch(_ int) {
	bphsWriteStats.recordSyncBatches.Add(1)
}

func recordBPHSRecordSyncQueued(requested, deduped int) {
	bphsWriteStats.recordSyncRequested.Add(uint64(requested))
	bphsWriteStats.recordSyncDeduped.Add(uint64(deduped))
}

func recordBPHSVLogSync(elapsed time.Duration) {
	bphsWriteStats.vlogSyncCalls.Add(1)
	bphsWriteStats.vlogSyncNs.Add(uint64(elapsed))
}

func recordBPHSSharedWAL(bytes int64, elapsed time.Duration) {
	bphsWriteStats.walSyncCalls.Add(1)
	bphsWriteStats.walSyncNs.Add(uint64(elapsed))
	if bytes > 0 {
		bphsWriteStats.walBytes.Add(uint64(bytes))
	}
}

func recordBPHSSharedWALReset() {
	bphsWriteStats.walResets.Add(1)
}

func recordBPHSMemtableFlush(elapsed time.Duration, bytes int64) {
	bphsWriteStats.memtableFlushes.Add(1)
	bphsWriteStats.memtableFlushNs.Add(uint64(elapsed))
	if bytes > 0 {
		bphsWriteStats.memtableFlushB.Add(uint64(bytes))
	}
}

func recordBPHSL0Merge(elapsed time.Duration) {
	bphsWriteStats.l0Merges.Add(1)
	bphsWriteStats.l0MergeNs.Add(uint64(elapsed))
}

func recordBPHSBaseCompaction(elapsed time.Duration) {
	bphsWriteStats.baseCompactions.Add(1)
	bphsWriteStats.baseCompactionNs.Add(uint64(elapsed))
}

func recordBPHSBloomProbe(skipped bool) {
	bphsWriteStats.bloomProbes.Add(1)
	if skipped {
		bphsWriteStats.bloomSkips.Add(1)
	}
}

type bphsReadSource uint8

const (
	bphsReadMem bphsReadSource = iota
	bphsReadL0
	bphsReadBase
	bphsReadMiss
)

func recordBPHSRead(source bphsReadSource) {
	if !bphsReadStatsEnabled {
		return
	}
	switch source {
	case bphsReadMem:
		bphsWriteStats.memHits.Add(1)
	case bphsReadL0:
		bphsWriteStats.l0Hits.Add(1)
	case bphsReadBase:
		bphsWriteStats.baseHits.Add(1)
	case bphsReadMiss:
		bphsWriteStats.readMisses.Add(1)
	}
}

func recordBPHSScan(partitions, l0Runs int) {
	if !bphsReadStatsEnabled {
		return
	}
	bphsWriteStats.scanQueries.Add(1)
	if partitions > 0 {
		bphsWriteStats.scanPartitions.Add(uint64(partitions))
	}
	if l0Runs > 0 {
		bphsWriteStats.scanL0Runs.Add(uint64(l0Runs))
	}
}

func bphsReadStatsLine() string {
	if !bphsReadStatsEnabled {
		return "[BPHS-READ] 无数据"
	}
	memHits := bphsWriteStats.memHits.Load()
	l0Hits := bphsWriteStats.l0Hits.Load()
	baseHits := bphsWriteStats.baseHits.Load()
	misses := bphsWriteStats.readMisses.Load()
	gets := memHits + l0Hits + baseHits + misses
	scanQueries := bphsWriteStats.scanQueries.Load()
	if gets == 0 && scanQueries == 0 {
		return "[BPHS-READ] 无数据"
	}
	avg := func(total, count uint64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count)
	}
	return fmt.Sprintf(
		"[BPHS-READ] gets=%d mem_hits=%d l0_hits=%d base_hits=%d misses=%d "+
			"mem_ratio=%.4f l0_ratio=%.4f base_ratio=%.4f | "+
			"scan_queries=%d scan_partitions_per_query=%.2f scan_l0_runs_per_query=%.2f",
		gets, memHits, l0Hits, baseHits, misses,
		avg(memHits, gets), avg(l0Hits, gets), avg(baseHits, gets),
		scanQueries,
		avg(bphsWriteStats.scanPartitions.Load(), scanQueries),
		avg(bphsWriteStats.scanL0Runs.Load(), scanQueries),
	)
}

func bphsWriteStatsLine() string {
	puts := bphsWriteStats.puts.Load()
	if puts == 0 {
		return "[BPHS-WRITE] 无数据"
	}
	batches := bphsWriteStats.syncBatches.Load()
	if batches == 0 {
		batches = 1
	}
	avgMs := func(total, count uint64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count) / 1e6
	}
	avgValue := func(total uint64, count uint64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count)
	}
	bloomProbes := bphsWriteStats.bloomProbes.Load()
	bloomSkips := bphsWriteStats.bloomSkips.Load()
	bloomSkipRate := 0.0
	if bloomProbes > 0 {
		bloomSkipRate = float64(bloomSkips) / float64(bloomProbes) * 100
	}
	return fmt.Sprintf(
		"[BPHS-WRITE] puts=%d puts_per_sync_batch=%.1f dirty_per_batch=%.2f sync_rounds_per_batch=%.2f "+
			"put_lock_wait=%.4fms partition_lock_wait=%.4fms sync_total=%.4fms/batch "+
			"record_sync_batches=%d record_sync_requested=%d record_sync_deduped=%d "+
			"record_sync_calls=%d record_sync=%.4fms/call vlog_sync_calls=%d vlog_sync=%.4fms/call "+
			"wal_sync_calls=%d wal_sync=%.4fms/call wal_mib=%.2f wal_resets=%d "+
			"mem_flushes=%d flush=%.4fms/call flush_mib=%.2f "+
			"l0_merges=%d l0_merge=%.4fms/call base_compactions=%d base_compact=%.4fms/call "+
			"bloom_probes=%d bloom_skips=%d bloom_skip_rate=%.2f%% avg_value=%.1fB",
		puts,
		avgValue(puts, batches),
		avgValue(bphsWriteStats.dirtyPartitions.Load(), batches),
		avgValue(bphsWriteStats.syncRounds.Load(), batches),
		avgMs(bphsWriteStats.putLockWaitNs.Load(), puts),
		avgMs(bphsWriteStats.partitionLockNs.Load(), bphsWriteStats.dirtyPartitions.Load()),
		avgMs(bphsWriteStats.syncTotalNs.Load(), batches),
		bphsWriteStats.recordSyncBatches.Load(),
		bphsWriteStats.recordSyncRequested.Load(),
		bphsWriteStats.recordSyncDeduped.Load(),
		bphsWriteStats.recordSyncCalls.Load(),
		avgMs(bphsWriteStats.recordSyncNs.Load(), bphsWriteStats.recordSyncCalls.Load()),
		bphsWriteStats.vlogSyncCalls.Load(),
		avgMs(bphsWriteStats.vlogSyncNs.Load(), bphsWriteStats.vlogSyncCalls.Load()),
		bphsWriteStats.walSyncCalls.Load(),
		avgMs(bphsWriteStats.walSyncNs.Load(), bphsWriteStats.walSyncCalls.Load()),
		float64(bphsWriteStats.walBytes.Load())/(1<<20),
		bphsWriteStats.walResets.Load(),
		bphsWriteStats.memtableFlushes.Load(),
		avgMs(bphsWriteStats.memtableFlushNs.Load(), bphsWriteStats.memtableFlushes.Load()),
		float64(bphsWriteStats.memtableFlushB.Load())/(1<<20),
		bphsWriteStats.l0Merges.Load(),
		avgMs(bphsWriteStats.l0MergeNs.Load(), bphsWriteStats.l0Merges.Load()),
		bphsWriteStats.baseCompactions.Load(),
		avgMs(bphsWriteStats.baseCompactionNs.Load(), bphsWriteStats.baseCompactions.Load()),
		bloomProbes,
		bloomSkips,
		bloomSkipRate,
		avgValue(bphsWriteStats.putBytes.Load(), puts),
	)
}
