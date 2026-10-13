package kvstore

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func tfKey(prefix string, i int) string { return fmt.Sprintf("%s-%08d", prefix, i) }

// 刚造出来的过滤器必须是保守的：新节点启动时尾部可能已经从盘上恢复出来了，
// 而位数组里什么都没有——此时若判"不在"就是漏报。
func TestTailFilterConservativeUntilMarkedExact(t *testing.T) {
	f := NewTailFilter(256, 10)
	if !f.MightContain("任何 key") {
		t.Fatal("尚未 MarkExact 时必须判「可能在」")
	}
	f.MarkExact()
	if f.MightContain("任何 key") {
		t.Fatal("MarkExact 之后、没 Add 过的 key 应判「不在」")
	}
	if _, _, _, cons := f.Stats(); cons {
		t.Fatal("MarkExact 之后不应再是保守状态")
	}
}

// 核心性质：加进去的 key 永远不能被判成"不在"。这是整个结构唯一不能让步的地方。
func TestTailFilterNoFalseNegatives(t *testing.T) {
	f := NewTailFilter(1024, 10) // 每段 1024 条 → 5000 条会跨 5 段
	f.MarkExact()
	const n = 5000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = tfKey("hit", i)
		f.Add(keys[i])
	}
	for _, k := range keys {
		if !f.MightContain(k) {
			t.Fatalf("漏报：%q 已经 Add 过，却判成「不在尾部」", k)
		}
	}
}

// 假阳性率要在量级上对得上 bits/key 的设定：10 bit/key 理论约 0.8%，
// 这里放宽到 3%，只用来挡住"哈希退化成常数"这类写坏。
func TestTailFilterFalsePositiveRate(t *testing.T) {
	const n = 100000
	// 段容量正好等于条数：位数组被填满，m/n 才真的是 10 bit/key。
	// 段只填一半的话 m/n 会翻倍，假阳性率会低得多，等于没验到 sizing。
	f := NewTailFilter(n, 10)
	f.MarkExact()
	for i := 0; i < n; i++ {
		f.Add(tfKey("in", i))
	}
	fp := 0
	for i := 0; i < n; i++ {
		if f.MightContain(tfKey("out", i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(n)
	if rate > 0.03 {
		t.Fatalf("假阳性率 %.3f 过高（10 bit/key 应在 1%% 量级）：哈希或位数组有问题", rate)
	}
}

// 段写满要自动封口开新段，否则段会无限长、逐段 OR 的收益就没了。
func TestTailFilterAutoSealOnFullSegment(t *testing.T) {
	f := NewTailFilter(4, 10)
	f.MarkExact()
	for i := 0; i < 4; i++ {
		f.Add(tfKey("k", i))
	}
	if segs, _, items, _ := f.Stats(); segs != 1 || items != 4 {
		t.Fatalf("4 条正好填满一段：segs=%d items=%d; want 1,4", segs, items)
	}
	f.Add(tfKey("k", 4))
	if segs, _, items, _ := f.Stats(); segs != 2 || items != 5 {
		t.Fatalf("第 5 条应触发封口：segs=%d items=%d; want 2,5", segs, items)
	}
}

// absorb 吃掉哪些段，那些段的 key 就应该被忘掉——这是"丢掉一个位数组"的实现。
func TestTailFilterDropThroughForgetsDroppedSegments(t *testing.T) {
	f := NewTailFilter(256, 10)
	f.MarkExact()
	for i := 0; i < 256; i++ {
		f.Add(tfKey("old", i))
	}
	for i := 0; i < 256; i++ {
		f.Add(tfKey("new", i))
	}
	boundary := f.SealSegment() // 封口段 1（装着 new*），当前变成段 2
	f.DropThrough(boundary)     // 丢 0 和 1
	if segs, _, _, _ := f.Stats(); segs != 1 {
		t.Fatalf("丢完之后应只剩一个空段，得到 %d 个", segs)
	}
	for i := 0; i < 256; i++ {
		if f.MightContain(tfKey("old", i)) {
			t.Fatalf("段 0 已被丢，old-%08d 不该判「可能在」", i)
		}
	}
}

// 安全关键：absorb 边界与段边界对齐。absorb 期间新写进来的 key 段号更大，
// 绝不能因为这次 absorb 的 DropThrough 被一起丢掉——那会让它读不到最新版本。
func TestTailFilterSealedBoundaryKeepsLaterWrites(t *testing.T) {
	f := NewTailFilter(4, 10)
	f.MarkExact()
	for i := 0; i < 4; i++ {
		f.Add(tfKey("old", i)) // 段 0
	}
	for i := 0; i < 4; i++ {
		f.Add(tfKey("mid", i)) // 自动封口段 0，进段 1
	}
	boundary := f.SealSegment() // 封口段 1，当前变成段 2
	for i := 0; i < 4; i++ {
		f.Add(tfKey("late", i)) // absorb 期间新写，落段 2
	}
	f.DropThrough(boundary)
	for i := 0; i < 4; i++ {
		if !f.MightContain(tfKey("late", i)) {
			t.Fatalf("late-%08d 是 absorb 期间写的，段号在边界之后，不能被丢", i)
		}
	}
}

// 崩溃/换主之后进保守状态：一律判"可能在"，功能不受影响，只是每次读跑两路。
func TestTailFilterInvalidateIsConservative(t *testing.T) {
	f := NewTailFilter(256, 10)
	f.MarkExact()
	for i := 0; i < 100; i++ {
		f.Add(tfKey("k", i))
	}
	f.Invalidate()
	if _, _, _, cons := f.Stats(); !cons {
		t.Fatal("Invalidate 之后应是保守状态")
	}
	for i := 0; i < 100; i++ {
		if !f.MightContain(tfKey("k", i)) {
			t.Fatalf("保守状态下 k-%08d 必须判「可能在」", i)
		}
	}
	if !f.MightContain("从来没见过的 key") {
		t.Fatal("保守状态下未 Add 过的 key 也必须判「可能在」")
	}
}

// 重建之后再放开，判定要回到精确。
func TestTailFilterRebuildThenMarkExact(t *testing.T) {
	f := NewTailFilter(256, 10)
	f.MarkExact()
	for i := 0; i < 100; i++ {
		f.Add(tfKey("k", i))
	}
	f.Invalidate()
	for i := 0; i < 100; i++ {
		f.Add(tfKey("k", i)) // 重建：重新扫一遍尾部
	}
	f.MarkExact()
	for i := 0; i < 100; i++ {
		if !f.MightContain(tfKey("k", i)) {
			t.Fatalf("重建后 k-%08d 应判「可能在」", i)
		}
	}
	if f.MightContain("从来没见过的 key") {
		t.Fatal("重建后未 Add 过的 key 应判「不在」")
	}
}

// 位数组的字节数要能对上 bits/key，这是估算内存占用的唯一依据。
func TestTailFilterBytesAccounting(t *testing.T) {
	f := NewTailFilter(1000, 10) // ceil(1000*10/64)=157 words
	if segs, b, _, _ := f.Stats(); segs != 1 || b != 157*8 {
		t.Fatalf("segs=%d bytes=%d; want 1,%d", segs, b, 157*8)
	}
}

// 并发：一个写者不断 Add，多个读者不断问"刚发布的那个 key 在不在"。
// 读者的纪律与真实读路径一致——**只问已经发布的 key**（Add 发生在 key 变得可读之前）。
// 这条配 -race 跑，同时挡住"读写不加锁"这类改动。
func TestTailFilterConcurrentPublishesAreVisible(t *testing.T) {
	f := NewTailFilter(1024, 10)
	f.MarkExact()
	const n = 20000
	keys := make([]string, n)
	for i := range keys {
		keys[i] = tfKey("concurrent", i)
	}
	var published atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			f.Add(keys[i])
			published.Store(int64(i + 1))
		}
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				k := int(published.Load())
				if k > 0 {
					// 最新发布的那条风险最高：它的位可能刚写下去。
					if !f.MightContain(keys[k-1]) {
						t.Errorf("漏报：已发布的 %q 判成「不在尾部」", keys[k-1])
						return
					}
				}
				if k >= n {
					return
				}
			}
		}()
	}
	wg.Wait()
}
