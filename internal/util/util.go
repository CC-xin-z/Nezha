// Package util holds the logging helpers and the random key/value generators shared by
// the node and the benchmark tools.
package util

import (
	"bytes"
	"log"
	"math/rand"
)

// Debug enables DPrintf.
//
// 关掉它是实测的结果，不是随手改的。
//
// 当一个 follower 的 nextIndex 落进已压缩区间时，raft.go 的 doAppendEntries 会走进
// 开头那个 start < 0 分支：跳过本轮、然后 armSync 让自己立刻重跑。条件不变，于是这
// 一轮原地打转——同一行调试日志以约 4.3 万行/秒、5.6 MB/s 的速度刷出来。
//
// 2026-10-15 那一晚的实测：node55 因此写出 118 GB 日志（data/3/n.log 一个文件），
// 把 1.6 T 的共享盘写满；进程随即写不进去而死（00:28 起 leader 侧持续 connection
// refused），三节点集群失去那个副本，剩下的 240+241 里 241 本身就是落后节点，凑不齐
// 健康的多数派——结果是 30% 的写 60 秒超时（goodPut 2639万/3754万）、28% 的 GET
// 返回 NOKEY、PUT 吞吐掉到 1/4。
//
// 关掉它不妨碍任何诊断：我们依赖的 [SYSTEM] / [TAIL-FILTER] / [TAIL-ROUTE] /
// [AVP-STATS] / [RECOVER] / GC 各阶段 / [LOCK-STALL] / [SLOW-APPEND] 全部走
// fmt.Printf，不经过这个开关。被它关掉的只有 compactLog 那类周期性调试行，以及上面
// 那条会自旋刷屏的。
//
// 注意这只治了"日志把盘写满"这一个症状：那个分支仍然以同样的频率自旋，只是不再落盘。
// 真正的修法是让它退避，而不是无条件 armSync——那是另一处改动。
const Debug = false

// DPrintf logs a debug line (prefix "[Debug]") when Debug is set.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetPrefix("[Debug] ")
		log.SetFlags(log.Ldate | log.Ltime)
		log.Printf(format, a...)
	}
	return
}

// EPrintf logs an error line (prefix "[Error]").
func EPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetPrefix("[Error] ")
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf(format, a...)
	return
}

// FPrintf logs a fatal-class line (prefix "[Fatalf]") without exiting.
func FPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetPrefix("[Fatalf] ")
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf(format, a...)
	return
}

// GenerateLargeValue returns a random lower-case string of size bytes.
func GenerateLargeValue(size int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteByte(letters[rand.Intn(len(letters))])
	}
	return buffer.String()
}

// GenerateFixedSizeKey returns a random string of size non-zero digits; callers prefix it
// with "key", so the first digit being non-zero keeps numeric keys distinct.
func GenerateFixedSizeKey(size int) string {
	const nonZeroLetters = "123456789"
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteByte(nonZeroLetters[rand.Intn(len(nonZeroLetters))])
	}
	return buffer.String()
}
