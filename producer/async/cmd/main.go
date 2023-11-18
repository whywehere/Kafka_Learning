package main

import (
	"go-kafka_example/conf"
	"go-kafka_example/producer/async"
)

// 本例展示最简单的 异步生产者 的使用
func main() {
	async.Producer(conf.Topic, 100_0000)
}
