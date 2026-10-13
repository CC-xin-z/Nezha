package kvstore

import "sync"

// TailFilter 是"尾部集合"的不漏报判定：回答一个 key **是否可能**还在尾部
// ——即尚未被 GC 搬进排序文件的那部分 valuelog。
//
// 它存在的理由不是"让查找更快"，而是**消掉一次白跑**。稳态下一次 GET 会并发查两处
// （valuelog 与排序文件），结果按优先级取——但**没被采用的那一路照样跑完**：read.go
// 里 st.spawn 起的 goroutine 把结果写进 buffered channel，没有 context 取消，所以
// 每次读都付两份查找，其中一份注定白费。有了"key 确定不在尾部"这个判定，就可以
// 只跑排序文件那一路。
//
// # 唯一硬要求：不能漏报
//
// valuelog 里的版本永远比排序文件里的新。若某个 key 其实在尾部、却判成"不在",
// 读就会拿到旧版本，破坏线性一致性。反过来"误报在尾部"只多跑一次 valuelog，无害。
// 也就是说，这里要的保证正好是 Bloom filter 的保证：**没有假阴性，只有假阳性**。
//
// 于是判定可以用近似、有界、易失的结构——而这是小值场景唯一付得起的形式：精确的
// per-key 索引要花掉它所覆盖数据的 β/v 倍内存（β ≈ 50~70 B 每条，64B 值时 β/v ≈ 0.7,
// 100GB 的数据就要 70GB 索引）。位数组只要 bits/key。
//
// # 为什么分段
//
// 尾部是 FIFO 的：GC 从最老的一端吸收，新数据从最新的一端追加。而 Bloom filter
// **不能删元素**——没法把"已经被搬走的那部分 key"从位数组里摘掉。所以把尾部切成段，
// 每段一个位数组：
//
//	seg0.log  seg0.bits     <- 最老，GC 下一个就吃它
//	...
//	segN.log  segN.bits     <- 最新，正在写
//
// 判定 = 逐段 OR。absorb 开始时封口当前段、开一个新段；absorb 成功后，丢弃所有完全
// 落在 absorb 边界之前的段——见 SealSegment / DropThrough。**absorb 边界必须与段
// 边界对齐**，否则会出现"一段里一半被搬走、一半还在"，而 Bloom 无法只删一半。
//
// # 安全退化
//
// 位数组不可用（尚未建好、校验失败、重启后尚未重建）的段一律判"可能在"，且不删它的
// 日志。于是这个结构**可以是纯易失的**：崩溃或换主之后先整段保守，后台重建；重建期间
// 功能完全正常，只是退化成两路并发（= 现状）。**它错了只会掉性能，不会出错。**
//
// 同理，NewTailFilter 造出来的过滤器一开始就是保守的：刚启动的节点尾部可能已经从盘上
// 恢复出来了，而位数组里什么都没有，此时若判"不在"就是漏报。要由调用方在尾部扫完
// （或确认尾部为空）之后调 RebuildDone 才放开。
//
// # 并发约定
//
// 写（apply 里的一次 Add）必须发生在该 key 变得可读之前。结构自带一把 RWMutex,
// 调用方只要守住上面那个顺序即可：现在 apply 与 captureState 都在 kvs.mu 下，
// 把 Add 放进 apply 那一段就满足。
type TailFilter struct {
	mu sync.RWMutex

	segItems int // 每段容纳多少 key 后自动封口
	segWords int // 每段位数组的字数 = ceil(segItems*bitsPerKey/64)
	hashes   int // 每个 key 置几位

	segs   []*tailSeg // 从老到新；最后一个是在写的那个
	nextNo int

	// conservative 为真时无论内容如何都判"可能在"。启动、换主、装快照之后置上，
	// 直到尾部被重新扫描再清掉。
	conservative bool
}

// tailSeg 是一个段的位数组。sealed 只作观测用——判定不看它，因为"这段还在不在"
// 由调用方通过 DropThrough 表达。
//
// 现在位数组只在内存里，所以不存在"某一段的位数组不可用"这种状态；整段不可信由
// conservative 统一表达。等位数组改成旁挂落盘（像分区的 .idx 那样）之后，载入失败
// 的单个段需要独立标成不可信，那时这里要补一个 per-seg 的 unusable 位。
type tailSeg struct {
	no     int
	bits   []uint64
	items  int
	sealed bool
}

const (
	// 10 bit/key ⇒ 假阳性约 0.8%。误报的代价是那一次读多跑一路 valuelog,而两路里
	// 本来就有一路白跑——所以这个代价相对收益是可以忽略的。
	defaultTailBitsPerKey = 10
	// 每段 26.2 万 key。1KB 值时约 1/4 GiB 尾部、位数组 320 KB；调这个值就是拿
	// "丢段的粒度"换"逐段 OR 的段数"——段数越多，判定越慢（每段一次位测试）。
	defaultTailSegItems = 1 << 18
)

// NewTailFilter 造一个保守（一律判"可能在"）的过滤器。段数从 1 开始，
// 写满自动封口开新段。bitsPerKey<=0 或 segItems<=0 时取默认值。
func NewTailFilter(segItems, bitsPerKey int) *TailFilter {
	if segItems <= 0 {
		segItems = defaultTailSegItems
	}
	if bitsPerKey <= 0 {
		bitsPerKey = defaultTailBitsPerKey
	}
	words := (segItems*bitsPerKey + 63) / 64
	if words < 1 {
		words = 1
	}
	// 最优哈希个数 k = (m/n)·ln2
	k := int(float64(bitsPerKey)*0.6931471805599453 + 0.5)
	if k < 1 {
		k = 1
	}
	if k > 32 {
		k = 32
	}
	f := &TailFilter{
		segItems:     segItems,
		segWords:     words,
		hashes:       k,
		conservative: true,
	}
	f.segs = append(f.segs, f.newSegLocked())
	return f
}

func (f *TailFilter) newSegLocked() *tailSeg {
	s := &tailSeg{no: f.nextNo, bits: make([]uint64, f.segWords)}
	f.nextNo++
	return s
}

// Add 把一个 key 记进当前段；当前段满了就封口开新段。
//
// 必须在 key 变得可读之前调用——否则读者可能问到一个位数组里还没有的 key,
// 那就是漏报。
func (f *TailFilter) Add(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seg := f.segs[len(f.segs)-1]
	if seg.items >= f.segItems {
		seg = f.sealLocked()
	}
	h1 := tailHash(key)
	h2 := (h1 >> 33) | (h1 << 31)
	h2 |= 1 // 取奇数：位数组长度是 64 的整数倍，奇数步长保证哈希序列遍历全部位置
	n := uint64(f.segWords * 64)
	for i := 0; i < f.hashes; i++ {
		p := (h1 + uint64(i)*h2) % n
		seg.bits[p/64] |= 1 << (p % 64)
	}
	seg.items++
}

// MightContain 回答"这个 key 是否可能还在尾部"。
//
// 返回 false 表示**确定不在**——调用方据此可以只查排序文件那一路。返回 true 只表示
// 可能在（含假阳性与一切安全退化的情形），调用方按老样子跑两路。
func (f *TailFilter) MightContain(key string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.conservative || len(f.segs) == 0 {
		return true
	}
	h1 := tailHash(key)
	h2 := (h1 >> 33) | (h1 << 31)
	h2 |= 1
	n := uint64(f.segWords * 64)
	// 从最新往老查：刚写过的 key 落在最新段里，早退的概率最高。
	for i := len(f.segs) - 1; i >= 0; i-- {
		s := f.segs[i]
		hit := true
		for j := 0; j < f.hashes; j++ {
			p := (h1 + uint64(j)*h2) % n
			if s.bits[p/64]&(1<<(p%64)) == 0 {
				hit = false
				break
			}
		}
		if hit {
			return true
		}
	}
	return false
}

// SealSegment 封口当前段并开一个新段，返回刚被封口那一段的段号。
//
// absorb 开始时调它，然后把这个段号当作 absorb 的边界：absorb 成功后用
// DropThrough(段号) 丢掉被搬走的那些段。**absorb 期间新写进来的数据落在新段里，
// 段号更大，因此不会被误丢**——这正是"absorb 边界与段边界对齐"的落点。
func (f *TailFilter) SealSegment() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	no := f.segs[len(f.segs)-1].no
	f.sealLocked()
	return no
}

func (f *TailFilter) sealLocked() *tailSeg {
	last := f.segs[len(f.segs)-1]
	last.sealed = true
	s := f.newSegLocked()
	f.segs = append(f.segs, s)
	return s
}

// DropThrough 丢弃所有段号 <= no 的段。absorb 成功之后调用。
//
// 段号比 no 大的段（absorb 期间写进来的）一律保留。若因此把所有段都丢了,
// 会补一个新段，保证"总是在写的那一段"始终存在。
func (f *TailFilter) DropThrough(no int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keep := f.segs[:0]
	for _, s := range f.segs {
		if s.no > no {
			keep = append(keep, s)
		}
	}
	f.segs = keep
	if len(f.segs) == 0 {
		f.segs = append(f.segs, f.newSegLocked())
	}
}

// Invalidate 进入保守状态并丢掉所有位数组：重建从这一步开始。
//
// 崩溃恢复、换主、装快照之后调用——此时位数组与盘上的尾部已经对不上，任何精确判定
// 都可能漏报。保守状态下功能完全正常，只是每次读都跑两路。
func (f *TailFilter) Invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conservative = true
	f.segs = []*tailSeg{f.newSegLocked()}
}

// MarkExact 声明重建完成：此后只按位数组判定。
//
// **调用方必须保证尾部里每一个 key 都已经 Add 过**（或者已经确认尾部为空）。
// 这个方法只翻标志，不清位数组——把"清空"和"放开"分成两步，就是为了不让中间那
// 一瞬间的"精确但空"变成漏报。
func (f *TailFilter) MarkExact() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conservative = false
}

// Stats 是观测口径：段数、位数组总字节、条数、是否保守。
//
// 字节数是估算内存占用的唯一依据，所以它就是位数组本身，不含每段的切片头与结构体。
func (f *TailFilter) Stats() (segments int, bitBytes int64, items int, conservative bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, s := range f.segs {
		bitBytes += int64(len(s.bits)) * 8
		items += s.items
	}
	return len(f.segs), bitBytes, items, f.conservative
}

// tailHash 是 FNV-1a 64 后面接一道 fmix64 收尾。
//
// 单用 FNV-1a 不够：它在乘法里把影响往上位扩散，**低位的雪崩本来就弱**，而判定用的
// 是 `h1 % 位数组长度`，吃的正是低位。键又都是 `key-00000123` 这种结构化字符串，
// 低位弱会让哈希挤在一起、假阳性率远高于设定值。fmix64 是 MurmurHash3 的收尾函数，
// 三条异或-乘法就把低位搅匀，每个键只多几次算术。
//
// 不要求跨进程稳定：位数组本来就是易失的，重建时重算一遍即可。
func tailHash(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
