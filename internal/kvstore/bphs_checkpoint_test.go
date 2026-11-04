package kvstore

import (
	"testing"
	"time"
)

// Regression test for the lock inversion that stalled live writes:
// applyUnderLock acquires kvs.mu and then stateMu.RLock. Maintenance must not
// hold stateMu first while waiting for kvs.mu.
func TestBPHSCheckpointDoesNotInvertApplyLockOrder(t *testing.T) {
	kvs := &KVServer{}
	kvs.mu.Lock()

	maintenanceDone := make(chan struct{})
	go func() {
		_, _, _, _ = kvs.checkpointBPHSLog(1)
		close(maintenanceDone)
	}()

	// Give the maintenance path a chance to take any locks that precede kvs.mu.
	time.Sleep(20 * time.Millisecond)

	applyPathCanContinue := make(chan struct{})
	go func() {
		kvs.stateMu.RLock()
		kvs.stateMu.RUnlock()
		close(applyPathCanContinue)
	}()

	select {
	case <-applyPathCanContinue:
		// Expected: apply's stateMu.RLock remains available while kvs.mu is held.
	case <-time.After(time.Second):
		kvs.mu.Unlock()
		t.Fatal("maintenance holds stateMu while waiting for kvs.mu; apply path deadlocks")
	}

	kvs.mu.Unlock()
	select {
	case <-maintenanceDone:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not finish after kvs.mu was released")
	}
}

func TestBPHSMaintenanceSelectsReadPriorityAfterWriteQuiet(t *testing.T) {
	now := time.Now()
	putAt := now.Add(-bphsReadCompactWriteQuiet - time.Second)
	readAt := now.Add(-time.Second)
	kvs := &KVServer{
		bphs:        &bphsStore{},
		lastPutTime: putAt,
	}
	kvs.lastClientActivity.Store(readAt.UnixNano())
	kvs.lastReadActivity.Store(readAt.UnixNano())
	kvs.postWriteReadRequests.Store(bphsReadCompactMinLoad)

	mode, gotPutAt := kvs.bphsMaintenanceModeLocked(now)
	if mode != bphsMaintenanceRead {
		t.Fatalf("maintenance mode=%v, want read-priority", mode)
	}
	if !gotPutAt.Equal(putAt) {
		t.Fatalf("maintenance put generation=%v, want %v", gotPutAt, putAt)
	}

	kvs.lastReadMaintainedPut = putAt
	if mode, _ := kvs.bphsMaintenanceModeLocked(now); mode != bphsMaintenanceNone {
		t.Fatalf("maintenance repeated for the same write generation: %v", mode)
	}

	kvs.lastPutTime = putAt.Add(time.Millisecond)
	if mode, _ := kvs.bphsMaintenanceModeLocked(now); mode != bphsMaintenanceRead {
		t.Fatalf("maintenance mode after a newer write=%v, want read-priority", mode)
	}
}

func TestBPHSMaintenanceRequiresRecentReadOrFullIdle(t *testing.T) {
	now := time.Now()
	kvs := &KVServer{
		bphs:        &bphsStore{},
		lastPutTime: now.Add(-bphsReadCompactWriteQuiet - time.Second),
	}

	kvs.lastClientActivity.Store(now.Add(-time.Second).UnixNano())
	kvs.lastReadActivity.Store(now.Add(-bphsReadCompactActiveFor - time.Second).UnixNano())
	if mode, _ := kvs.bphsMaintenanceModeLocked(now); mode != bphsMaintenanceNone {
		t.Fatalf("stale read activity selected maintenance: %v", mode)
	}

	kvs.lastClientActivity.Store(now.Add(-bphsIdleCompactQuiet - time.Second).UnixNano())
	kvs.lastPutTime = now.Add(-bphsIdleCompactQuiet - time.Second)
	if mode, _ := kvs.bphsMaintenanceModeLocked(now); mode != bphsMaintenanceIdle {
		t.Fatalf("fully idle store selected mode=%v, want idle", mode)
	}
}
