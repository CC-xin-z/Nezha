package kvstore

import (
	"fmt"
	"sync/atomic"
	"time"
)

// bphsWriteStats separates BPHS's extra durability work from the common Raft
// path. The key values are dirty partitions per apply batch and the resulting
// number of partition fsync rounds.
var bphsWriteStats struct {
	puts             atomic.Uint64
	putBytes         atomic.Uint64
	putLockWaitNs    atomic.Uint64
	syncBatches      atomic.Uint64
	dirtyPartitions  atomic.Uint64
	syncRounds       atomic.Uint64
	partitionLockNs  atomic.Uint64
	recordSyncCalls  atomic.Uint64
	recordSyncNs     atomic.Uint64
	vlogSyncCalls    atomic.Uint64
	vlogSyncNs       atomic.Uint64
	syncTotalNs      atomic.Uint64
	memtableFlushes  atomic.Uint64
	memtableFlushNs  atomic.Uint64
	memtableFlushB   atomic.Uint64
	l0Merges         atomic.Uint64
	l0MergeNs        atomic.Uint64
	baseCompactions  atomic.Uint64
	baseCompactionNs atomic.Uint64
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

func recordBPHSVLogSync(elapsed time.Duration) {
	bphsWriteStats.vlogSyncCalls.Add(1)
	bphsWriteStats.vlogSyncNs.Add(uint64(elapsed))
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
	return fmt.Sprintf(
		"[BPHS-WRITE] puts=%d puts_per_sync_batch=%.1f dirty_per_batch=%.2f sync_rounds_per_batch=%.2f "+
			"put_lock_wait=%.4fms partition_lock_wait=%.4fms sync_total=%.4fms/batch "+
			"record_sync_calls=%d record_sync=%.4fms/call vlog_sync_calls=%d vlog_sync=%.4fms/call "+
			"mem_flushes=%d flush=%.4fms/call flush_mib=%.2f "+
			"l0_merges=%d l0_merge=%.4fms/call base_compactions=%d base_compact=%.4fms/call avg_value=%.1fB",
		puts,
		avgValue(puts, batches),
		avgValue(bphsWriteStats.dirtyPartitions.Load(), batches),
		avgValue(bphsWriteStats.syncRounds.Load(), batches),
		avgMs(bphsWriteStats.putLockWaitNs.Load(), puts),
		avgMs(bphsWriteStats.partitionLockNs.Load(), bphsWriteStats.dirtyPartitions.Load()),
		avgMs(bphsWriteStats.syncTotalNs.Load(), batches),
		bphsWriteStats.recordSyncCalls.Load(),
		avgMs(bphsWriteStats.recordSyncNs.Load(), bphsWriteStats.recordSyncCalls.Load()),
		bphsWriteStats.vlogSyncCalls.Load(),
		avgMs(bphsWriteStats.vlogSyncNs.Load(), bphsWriteStats.vlogSyncCalls.Load()),
		bphsWriteStats.memtableFlushes.Load(),
		avgMs(bphsWriteStats.memtableFlushNs.Load(), bphsWriteStats.memtableFlushes.Load()),
		float64(bphsWriteStats.memtableFlushB.Load())/(1<<20),
		bphsWriteStats.l0Merges.Load(),
		avgMs(bphsWriteStats.l0MergeNs.Load(), bphsWriteStats.l0Merges.Load()),
		bphsWriteStats.baseCompactions.Load(),
		avgMs(bphsWriteStats.baseCompactionNs.Load(), bphsWriteStats.baseCompactions.Load()),
		avgValue(bphsWriteStats.putBytes.Load(), puts),
	)
}
