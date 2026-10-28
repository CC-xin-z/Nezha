package kvstore

// bphsKeyFilter is an in-memory Bloom filter for one immutable BPHS L0 file.
// L0 runs contain only a sparse subset of the partition's key space, so without
// a filter a Base hit must read and probe every newer L0 file first.
type bphsKeyFilter struct {
	bits []uint64
	mask uint64
}

const (
	bphsBloomBitsPerKey = 10
	bphsBloomHashCount  = 7
)

func newBPHSKeyFilterFromRecords(records []bphsRecord) *bphsKeyFilter {
	keys := make([]string, 0, len(records))
	for i := range records {
		keys = append(keys, records[i].Key)
	}
	return newBPHSKeyFilter(keys)
}

func newBPHSKeyFilter(keys []string) *bphsKeyFilter {
	if len(keys) == 0 {
		return nil
	}
	wantBits := uint64(len(keys) * bphsBloomBitsPerKey)
	bitCount := uint64(64)
	for bitCount < wantBits {
		bitCount <<= 1
	}
	filter := &bphsKeyFilter{
		bits: make([]uint64, bitCount/64),
		mask: bitCount - 1,
	}
	for _, key := range keys {
		filter.Add(key)
	}
	return filter
}

func (f *bphsKeyFilter) Add(key string) {
	if f == nil {
		return
	}
	h1, h2 := bphsBloomHashes(key)
	for i := uint64(0); i < bphsBloomHashCount; i++ {
		bit := (h1 + i*h2) & f.mask
		f.bits[bit>>6] |= uint64(1) << (bit & 63)
	}
}

// MayContain may return one for an absent key (a Bloom-filter false positive),
// but never returns zero for a key that was written into this file.
func (f *bphsKeyFilter) MayContain(key string) bool {
	if f == nil {
		return true
	}
	h1, h2 := bphsBloomHashes(key)
	for i := uint64(0); i < bphsBloomHashCount; i++ {
		bit := (h1 + i*h2) & f.mask
		if f.bits[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

func bphsBloomHashes(key string) (uint64, uint64) {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	h1 := bphsBloomMix(h)
	h2 := bphsBloomMix(h1 ^ 0x9e3779b97f4a7c15)
	h2 |= 1 // double hashing must have a non-zero step
	return h1, h2
}

func bphsBloomMix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
