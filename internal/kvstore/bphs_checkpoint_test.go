package kvstore

import (
	"testing"
	"time"
)

// Regression test for the lock inversion that stalled live writes:
// applyUnderLock acquires kvs.mu and then stateMu.RLock. Maintenance must not
// hold stateMu first while waiting for kvs.mu.
func TestBPHSDurableDrainDoesNotInvertApplyLockOrder(t *testing.T) {
	kvs := &KVServer{}
	kvs.mu.Lock()

	maintenanceDone := make(chan struct{})
	go func() {
		_, _ = kvs.drainBPHSDurable(1)
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
