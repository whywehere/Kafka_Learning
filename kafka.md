# :heart: Kafka Learning

kafka学习笔记

## 一、消息队列

>**消息队列**是一种异步的服务间通信方式，适用于无服务器和微服务架构。 **消息**在被处理和删除之前一直存储在**队列**上。 每条**消息**仅可被一位用户处理一次。 **消息队列**可被用于分离重量级处理、缓冲或批处理工作以及缓解高峰期工作负载。

### 1.1 架构

![image-20231118110048560](C:\Users\19406\AppData\Roaming\Typora\typora-user-images\image-20231118110048560.png)

只要是用队列的形式将消息临时存储起来，然后一边写，一边读，就可以看做是个消息队列。写入的一方我们成为**生产者**，读取的一端我们成为**消费者**。

### 1.2 消息队列的作用

#### 1.2.1 削峰填谷

这个可能是使用最多的情况。当上游的流量冲击过大， 想要不被打垮，要么抛弃这些流量，要么存储这些流量。很显然，存起来在大多数时候是更好地选择，注意这里是大多数，有时候一些具有时效性的场景，抛弃可能反而更好。

将流量存起来，在空闲的时候慢慢消费，缓冲上下游的瞬时突发流量，使其更平滑，增加系统的可用性，这就是削峰填谷。

#### 1.2.2 解耦

通常我们的代码是存在调用链的，如果是一个单体里面，直接函数调用，等待返回就好了。但是在分布式、微服务中，我们可能不知道上下游的改动或者可靠性。

举个例子，A服务产生了一个事件，比如注册，调用了BCD三个服务，比如发短信、初始化用户缓存等操作。

那么问题就来了：

- 如果需要添加E或者删除C，我们需要修改A的代码。
- 如果B挂了，ACD需要等它重启好了再开始工作么?如果不等，怎么补发消息？
- 如果D处理的很慢，ABC需要等待它么？

如果我们使用发布订阅模型，将它们解耦，A只用发布消息，BCD去订阅消息，添加E只要它去添加订阅即可。A不用关心消费者的速度或者是否存在，消息的存储由消息队列负责，等它们自己重启了再去消费即可。

#### 1.2.3 异步

和解耦的案例很像，我们可以将ABCD这种顺序执行的事情改成A发完消息后就返回，BCD自己异步消费的模式，这样就使得A不用被不重要的事情拖慢处理速度。

问题：顺序性。如果逻辑是要顺序执行，若并发可能导致乱序。

### 1.3 消息队列的缺点

**可用性在初期是降低的。** 因为加入了新系统，必然会带来不稳定，尤其是消息队列往往会成为系统的核心，崩溃后带来的问题是范围极大的。但是一旦建设好了，整体的可用性会增加。

**复杂度增加。** 需要解决消息丢失、重复消费、顺序性等问题。

**一致性问题。** 也叫幂等性，即生产者生产的消息，消费者应该必然且只消费一次，哪怕多次消费，也应该和只消费了一次一样。

>  消息队列的三大难题：
>
> - **消息重复消费。** 即一条消息只应被消费一次。
> - **消息丢失。** 即消息应该被收到且消费。
> - **消息的顺序消费。** 即消息应该被顺序消费。

### 1.4 消息的两种模型 Pull VS Push

Pull模式即消费者自己拉取，这样做的好处是**消费者可以决定自己的速度**。缺点是消费者需要轮训有没有新消息，如果消费者太慢，可能会反压上游。

Push模式就比较粗暴了，和发短信一样，直接消息队列推送，并不会管消费者的死活。这种方式显而易见**会导致消息丢失**，但是好处就是消息队列压力很小。

## 二、Kafka整体架构

>Kafka的优势：
>
>- kafka是将消息存储在硬盘上，以追加写和零拷贝来提升效率。这样带来的好处就是廉价、高效、可靠，可以无限回放。（很适合做海量的消息存储和分析，硬盘比内存便宜太多了）
>- 保障了单个partition上的消息顺序。
>- 通过消息分区，分布式消费，使得扩容很简单，同时多副本保障了可用性。
>- 具有消息压缩功能，极大节省了带宽。（消息队列的瓶颈绝对在带宽上，而不是硬盘、CPU或者内存上。）

![](C:\Users\19406\Desktop\学习资料\kafka-1.png)

一个典型的 Kafka 体系架构包括若干 Producer、若干 Broker、若干 Consumer，以及一个ZooKeeper 集群，其中ZooKeeper 是 Kafka 用来负责集群元数据的管理、控制器的选举等操作的。Producer 将消息发送到 Broker，Broker 负责将收到的消息存储到磁盘中，而 Consumer 负责从 Broker 订阅并消费消息。

整个 Kafka 体系结构中引入了以下 3 个术语。

- **Producer：生产者**，也就是发送消息的一方。生产者负责创建消息，然后将其投递到 Kafka 中。

- **Consumer：消费者**，也就是接收消息的一方。消费者连接到 Kafka 上并接收消息，进 而进行相应的业务逻辑处理。

- **Broker：服务代理节点**，Broker 可以简单地看作一个独立的 Kafka 服务节点或 Kafka 服务实例。大多数情况下也可以将 Broker 看作一台 Kafka 服务器，前提是这台服务器上只部署了一个 Kafka 实例。一个或多个 Broker 组成了一个 Kafka 集群。一般而言， 我们更习惯使用首字母小写的 broker 来表示服务代理节点。

## 三、Topic、Partition

Kafka 中的消息以 topic 为单位进行归类，producer负责将消息发送到特定的 topic (发送到 Kafka 集群中的每一条消息都要指定一个主题)，而消费者负责订阅主题并进行消费。

<u>一个topic可以细分为多个分区， 一个分区只属于单个topic</u>，partition 也称 **Topic-Partition** 。主题下的每条消息只会保存在某一个分区中，而不会在多个分区中被保存多份。同一topic下不同分区包含的消息是不同的，分区在存储层面可以看作一个可追加的日志（Log）文件，消息在被追加到分区日志文件的时候都会分配一个特定的偏移量（offset）。**offset** 是消息在分区中的唯一标识，Kafka 通过它来保证消息在分区内的顺序性，不过 offset 并不跨越分区，也就是说，Kafka 保证的是分区有序而不是主题有序。

![](C:\Users\19406\Desktop\学习资料\kafka-2.png)

如上图所示，主题中有 3 个分区，消息被顺序追加到每个分区日志文件的尾部。

Kafka 中的分区可以分布在不同的服务器（broker）上，也就是说，一个主题可以横跨多个 broker，以此来提供比单个 broker 更强大的性能。

每一条消息被发送到 broker 之前，会根据**分区规则**选择被存储到哪个具体的分区。如果分区规则设定得合理，所有的消息都可以均匀地分配在不同的分区中。如果一个主题只对应一个文件，那么这个文件所在的机器 I/O 将会成为这个主题的性能瓶颈，而分区解决了这个问题。 在创建主题的时候可以通过指定的参数来设置分区的个数，当然也可以在主题创建完成之后去修改分区的数量，通过增加分区的数量可以实现水平扩展。

## 四、Consumer And Consumer Group

![](C:\Users\19406\Desktop\学习资料\kafka-3.png)

每个消费者都有一个对应的消费组。当消息发布到主题后，只会被投递给订阅它的每个消费组中的一个消费者。

如上图所示，某个主题中共有 4 个分区（Partition）: P0、P1、P2、P3。有两个消费组 A 和 B 都订阅了这个主题，消费组 A 中有 4 个消费者（C0、C1、C2 和 C3），消费组 B 中有 2 个消费者（C4 和 C5）。按照 Kafka 默认的规则，最后的分配结果是消费组 A 中的每一个消费者分配到 1 个分区，消费组 B 中的每一个消费者分配到 2 个分区，两个消费组之间互不影响。 每个消费者只能消费所分配到的分区中的消息。换言之，每一个分区只能被一个消费组中的一 个消费者所消费。

## 五、存储视图

![](C:\Users\19406\Desktop\学习资料\kafka-4.png)

主题和分区都是提供给上层用户的抽象，而在副本层面或更加确切地说是 Log 层面才有实际物理上的存在。同一个分区中的多个副本必须分布在不同的 broker 中，这样才能提供有效的数据冗余。

为了防止 Log 过大， Kafka 又引入了日志分段（LogSegment）的概念，将 Log 切分为多个 LogSegment，相当于一个巨型文件被平均分配为多个相对较小的文件，这样也便于消息的维护和清理。

## 六、多副本(Replica)

Kafka 为分区引入了多副本（Replica）机制，通过增加副本数量可以提升容灾能力。同一partition的不同replica中保存的是相同的消息（在同一时刻，副本之间并非完全一样），replica之间是一主多从的关系，其中 **leader 副本负责处理读写请求**，**follower 副本只负责与 leader 副本的消息同步**。副本处于不同的 broker 中，当 leader 副本出现故障时，从 follower 副本中重新选举新的 leader 副本对外提供服务。Kafka 通过多副本机制实现了故障的自动转移，当 Kafka 集群中某个 broker 失效时仍然能保证服务可用。

![image-20231118113139242](C:\Users\19406\AppData\Roaming\Typora\typora-user-images\image-20231118113139242.png)

副本自身是没有专门的编号的，副本在哪个 Broker 上，对应的 Broker ID 就是它的编号（这里也间接限制了副本数量的最大值必须小于 Broker 节点数量）

分区中的所有副本统称为 **AR** **(Assigned Replicas)** 。所有与 leader 副本保持一定程度同步的副本(包括 leader 副本在内)组成 **ISR (In-Sync Replicas)** ，ISR 集合是 AR 集合中的一个子 集。消息会先发送到 leader 副本，然后 follower 副本才能从 leader 副本中拉取消息进行同步， 同步期间内 follower 副本相对于 leader 副本而言会有一定程度的滞后。

前面所说的“一定程度的同步”是指可忍受的滞后范围，这个范围可以通过参数进行配置。与 leader 副本同步滞后过多的副本(不包括 leader 副本)组成 **OSR (Out-of-Sync Replicas)** ，由此可见，**AR** **=ISR+OSR**。 在正常情况下，所有的 follower 副本都应该与 leader 副本保持一定程度的同步，即 AR=ISR， OSR 集合为空。

leader 副本负责维护和跟踪 ISR 集合中所有 follower 副本的滞后状态，当 follower 副本落后太多或失效时，leader 副本会把它从 ISR 集合中剔除。如果 OSR 集合中有 follower 副本“追上” 了leader 副本，那么 leader 副本会把它从 OSR 集合转移至 ISR 集合。<u>默认情况下，当 leader 副本发生故障时，只有在 ISR 集合中的副本才有资格被选举为新的 leader，而在 OSR 集合中的副本则没有任何机会</u>(不过这个原则也可以通过修改相应的参数配置来改变)。

![](C:\Users\19406\Desktop\学习资料\kafka-7.png)

ISR 与 HW 和 LEO 也有紧密的关系。**HW** 是 High Watermark 的缩写，俗称高水位，它标识 了一个特定的消息偏移量(offset)，消费者只能拉取到这个 offset 之前的消息。

如上图所示，它代表一个日志文件，这个日志文件中有 9 条消息，第一条消息的offset

（LogStartOffset）为 0，最后一条消息的 offset 为 8，offset 为 9 的消息用虚线框表示，代表下一条待写入的消息。日志文件的 HW 为 6，表示消费者只能拉取到 offset 在 0 至 5 之间的消息， 而 offset 为 6 的消息对消费者而言是不可见的。

**LEO** 是 Log End Offset 的缩写，它标识当前日志文件中下一条待写入消息的 offset，上图中 offset 为 9 的位置即为当前日志文件的 LEO，LEO 的大小相当于当前日志分区中最后一条消息的 offset 值加 1。分区 ISR 集合中的每个副本都会维护自身的 LEO，而 ISR 集合中最小的 LEO 即为分区的 HW，对消费者而言只能消费 HW 之前的消息。

>那么我们想要顺序消费，也要提升消费速度，怎么办？
>
>- 如果两个消费者同时消费同一个topic下的同一个partition，很显然，他们会重复消费。因为每个消费者的offest是独立保存的。
>- 如果我们分成两个partition，假设topic的数据是123456， 采用随机分配的策略，partition1上的可能是135,2上面是246,消费者A读取1，B读取2，这样就不会重复消费了，但是如果A的速度很快，可能A都到5了，B的2还没读完。这就导致了乱序消费。
>- 很简单，在上面的方案中，我们将随机分配改成哈希分配，从业务层将一个业务逻辑的消息发送到同一个partition上，比如用户ID。如果你的运气足够不好，可能会出现一个partition消息多，另一个少的情况。
>
>

## 七、消息可靠性分析

就 Kafka 而言，越多的副本数越能够保证数据的可靠性，副本数可以在创建主题时配置， 也可以在后期修改，不过副本数越多也会引起磁盘、网络带宽的浪费，同时会引起性能的下降。 一般而言，设置副本数为 3 即可满足绝大多数场景对可靠性的要求，而对可靠性要求更高的场 景下，可以适当增大这个数值，比如国内部分银行在使用 Kafka 时就会设置副本数为 5。

大多数人还会想到生产者客户端参数 acks。 对于这个参数而言: 相比于 0 和 1，acks = -1(客户端还可以配置为 all，它的含 义与-1 一样，以下只以-1 来进行陈述)可以最大程度地提高消息的可靠性。

对于 acks = 1 的配置，生产者将消息发送到 leader 副本，leader 副本在成功写入本地日志之 后会告知生产者已经成功提交

![](C:\Users\19406\Desktop\学习资料\kafka-8.png)

对于 ack = -1 的配置，生产者将消息发送到 leader 副本，leader 副本在成功写入本地日志之 后还要等待 ISR 中的 follower 副本全部同步完成才能够告知生产者已经成功提交，即使此时 leader 副本宕机，消息也不会丢失，如图所示。

![](C:\Users\19406\Desktop\学习资料\kafka-9.png)

消息发送有 3 种模式，即**发后即忘、同步和异步**。对于发后即忘的模式，不管消息有没有被成功写入，生产者都不会收到通知，那么即使消息写入失败也无从 得知，因此<u>发后即忘的模式不适合高可靠性要求的场景</u>。如果要提升可靠性，那么生产者可以 采用同步或异步的模式，在出现异常情况时可以及时获得通知，以便可以做相应的补救措施， 比如选择重试发送(可能会引起消息重复)。

## 八、Golang + Kafka

`github.com/IBM/sarama`

### ProduceDemo

```go
// asyncProducer demo
func Producer(topic string, limit int) {
	config := sarama.NewConfig()
	// 异步生产者不建议把 Errors 和 Successes 都开启，一般开启 Errors 就行
	// 同步生产者就必须都开启，因为会同步返回发送成功或者失败
	config.Producer.Return.Errors = true    // 设定是否需要返回错误信息
	config.Producer.Return.Successes = true // 设定是否需要返回成功信息
	producer, err := sarama.NewAsyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	var (
		wg                                   sync.WaitGroup
		enqueued, timeout, successes, errors int
	)
	// [!important] 异步生产者发送后必须把返回值从 Errors 或者 Successes 中读出来 不然会阻塞 sarama 内部处理逻辑 导致只能发出去一条消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			// log.Printf("[Producer] Success: key:%v msg:%+v \n", s.Key, s.Value)
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range producer.Errors() {
			log.Printf("[Producer] Errors：err:%v msg:%+v \n", e.Msg, e.Err)
			errors++
		}
	}()

	// 异步发送
	for i := 0; i < limit; i++ {
		str := strconv.Itoa(int(time.Now().UnixNano()))
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(str)}
		/*
			异步发送只是写入内存了就返回了，并没有真正发送出去
			sarama 库中用的是一个 channel 来接收，后台 goroutine 异步从该 channel 中取出消息并真正发送
			select + ctx 做超时控制,防止阻塞 producer.Input() <- msg 也可能会阻塞
		*/
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		select {
		case producer.Input() <- msg:
			enqueued++
		case <-ctx.Done():
			timeout++
		}
		cancel()
		if i%10000 == 0 && i != 0 {
			log.Printf("已发送消息数:%d 超时数:%d\n", i, timeout)
		}
	}
	log.Printf("发送完毕 总发送条数:%d enqueued:%d timeout:%d successes: %d errors: %d\n", limit, enqueued, timeout, successes, errors)
	// close
	producer.AsyncClose()
	wg.Wait()
}

// SyncProducerDemo
func Producer(topic string, limit int) {
	config := sarama.NewConfig()
	// 同步生产者必须同时开启 Return.Successes 和 Return.Errors,因为同步生产者在发送之后就必须返回状态，所以需要两个都返回
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true // 这个默认值就是 true 可以不用手动 赋值
	/*
		同步生产者和异步生产者逻辑是一致的，Success或者Errors都是通过channel返回的，
		只是同步生产者封装了一层，等channel返回之后才返回给调用者
		具体见 sync_producer.go 文件72行 newSyncProducerFromAsyncProducer 方法
		内部启动了两个 goroutine 分别处理Success Channel 和 Errors Channel
		同步生产者内部就是封装的异步生产者
		type syncProducer struct {
			producer *asyncProducer
			wg       sync.WaitGroup
		}
	 */
	producer, err := sarama.NewSyncProducer([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewSyncProducer err:", err)
	}
	defer producer.Close()
	var successes, errors int
	for i := 0; i < limit; i++ {
		str := strconv.Itoa(int(time.Now().UnixNano()))
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(str)}
		partition, offset, err := producer.SendMessage(msg) // 发送逻辑也是封装的异步发送逻辑，可以理解为将异步封装成了同步
		if err != nil {
			log.Printf("SendMessage:%d err:%v\n ", i, err)
			errors++
			continue
		}
		successes++
		log.Printf("[Producer] partitionid: %d; offset:%d, value: %s\n", partition, offset, str)
	}
	log.Printf("发送完毕 总发送条数:%d successes: %d errors: %d\n", limit, successes, errors)
}
```

#### 发送流程源码分析

> 为了便于阅读，省略了部分无关代码。

另外：由于同步生产者和异步生产者逻辑是一致的，只是在异步生产者基础上封装了一层，所以本文主要分析了异步生产者。

```go
// 可以看到 同步生产者其实就是把异步生产者封装了一层
type syncProducer struct {
    producer *asyncProducer
    wg       sync.WaitGroup
}
```

>   **NewAsyncProducer**

首先是构建一个异步生产者对象

```go
func NewAsyncProducer(addrs []string, conf *Config) (AsyncProducer, error) {
	client, err := NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}
	return newAsyncProducer(client)
}

func newAsyncProducer(client Client) (AsyncProducer, error) {
	// ...
	p := &asyncProducer{
		client:     client,
		conf:       client.Config(),
		errors:     make(chan *ProducerError),
		input:      make(chan *ProducerMessage),
		successes:  make(chan *ProducerMessage),
		retries:    make(chan *ProducerMessage),
		brokers:    make(map[*Broker]*brokerProducer),
		brokerRefs: make(map[*brokerProducer]int),
		txnmgr:     txnmgr,
	}

	go withRecover(p.dispatcher)
	go withRecover(p.retryHandler)
}
```

可以看到在 `newAsyncProducer` 最后开启了两个 goroutine，一个为 `dispatcher`，一个为 `retryHandler`。retryHandler 主要是处理重试逻辑，暂时先忽略。

>  **dispatcher**

主要根据 `topic` 将消息分发到对应的 channel。

```go
func (p *asyncProducer) dispatcher() {
   handlers := make(map[string]chan<- *ProducerMessage)
   // ...
   for msg := range p.input {
       
	  // 拦截器逻辑
      for _, interceptor := range p.conf.Producer.Interceptors {
         msg.safelyApplyInterceptor(interceptor)
      }
	  // 找到这个Topic对应的Handler
      handler := handlers[msg.Topic]
      if handler == nil {
         // 如果没有这个Topic对应的Handler，那么创建一个
         handler = p.newTopicProducer(msg.Topic)
         handlers[msg.Topic] = handler
      }
	  // 然后把这条消息写进这个Handler中
      handler <- msg
   }
}
```

具体逻辑：从 `p.input` 中取出消息并写入到 `handler` 中，如果 `topic` 对应的 `handler` 不存在，则调用 `newTopicProducer()` 创建。

> 这里的 handler 是一个 buffered channel

然后让我们来看下`handler = p.newTopicProducer(msg.Topic)`这一行的代码。

```go
func (p *asyncProducer) newTopicProducer(topic string) chan<- *ProducerMessage {
   input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
   tp := &topicProducer{
      parent:      p,
      topic:       topic,
      input:       input,
      breaker:     breaker.New(3, 1, 10*time.Second),
      handlers:    make(map[int32]chan<- *ProducerMessage),
      partitioner: p.conf.Producer.Partitioner(topic),
   }
   go withRecover(tp.dispatch)
   return input
}
```

在这里创建了一个缓冲大小为`ChannelBufferSize`的channel，用于存放发送到这个主题的消息，然后创建了一个 `topicProducer`。

> 在这个时候你可以认为消息已经交付给各个 topic 对应的 topicProducer 了。

还有一个需要注意的是`newTopicProducer` 的这种写法，内部创建一个 chan 返回到外层，然后通过在内部新开一个 goroutine 来处理该 chan 里的消息，这种写法在后面还会遇到好几次。

> 相比之下在外部显示创建 chan 之后传递到该函数可能会更容易理解。

 **topicDispatch**

`newTopicProducer`的最后一行`go withRecover(tp.dispatch)`又启动了一个 goroutine 用于处理消息。也就是说，到了这一步，对于每一个Topic，都有一个协程来处理消息。

dispatch 具体如下：

```go
func (tp *topicProducer) dispatch() {
	for msg := range tp.input {
		handler := tp.handlers[msg.Partition]
		if handler == nil {
			handler = tp.parent.newPartitionProducer(msg.Topic, msg.Partition)
			tp.handlers[msg.Partition] = handler
		}

		handler <- msg
	}
}
```

可以看到又是同样的套路：

- 1）找到这条消息所在的分区对应的 channel，然后把消息丢进去
- 2）如果不存在则新建 chan

##### PartitionDispatch

新建的 chan 是通过 `newPartitionProducer` 返回的，和之前的`newTopicProducer`又是同样的套路,点进去看一下：

```go
func (p *asyncProducer) newPartitionProducer(topic string, partition int32) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	pp := &partitionProducer{
		parent:    p,
		topic:     topic,
		partition: partition,
		input:     input,

		breaker:    breaker.New(3, 1, 10*time.Second),
		retryState: make([]partitionRetryState, p.conf.Producer.Retry.Max+1),
	}
	go withRecover(pp.dispatch)
	return input
}
```

> 果然是这样，有没有一种似曾相识的感觉。

`TopicProducer`是按照 `Topic` 进行分发，这里的 `PartitionProducer` 则是按照 `partition` 进行分发。

> 到这里可以认为消息已经交付给对应 topic 下的对应 partition 了。

每个 partition 都会有一个 goroutine 来处理分发给自己的消息。

##### PartitionProducer

到了这一步，我们再来看看消息到了每个 partition 所在的 channel 之后，是如何处理的。

> 其实在这一步中，主要是做一些错误处理之类的，然后把消息丢进brokerProducer。

可以理解为这一步是业务逻辑层到网络IO层的转变，在这之前我们只关心消息去到了哪个分区，而在这之后，我们需要找到这个分区所在的 broker 的地址，并使用之前已经建立好的 TCP 连接，发送这条消息。

具体 `pp.dispatch` 代码如下

```go
func (pp *partitionProducer) dispatch() {
	// 找到这个主题和分区的leader所在的broker
	pp.leader, _ = pp.parent.client.Leader(pp.topic, pp.partition)
	if pp.leader != nil {
        // 根据 leader 信息创建一个 BrokerProducer 对象
		pp.brokerProducer = pp.parent.getBrokerProducer(pp.leader)
		pp.parent.inFlight.Add(1) 
		pp.brokerProducer.input <- &ProducerMessage{Topic: pp.topic, Partition: pp.partition, flags: syn}
	}
	// 然后把消息丢进brokerProducer中
	for msg := range pp.input {
		pp.brokerProducer.input <- msg
	}
}
```

> 根据之前的套路我们知道，真正的逻辑肯定在`pp.parent.getBrokerProducer(pp.leader)` 这个方法里面。

##### BrokerProducer

到了这里，大概算是整个发送流程最后的一个步骤了。

让我们继续跟进`pp.parent.getBrokerProducer(pp.leader)`这行代码里面的内容。其实就是找到`asyncProducer`中的`brokerProducer`，如果不存在，则创建一个。

```go
func (p *asyncProducer) getBrokerProducer(broker *Broker) *brokerProducer {
	p.brokerLock.Lock()
	defer p.brokerLock.Unlock()

	bp := p.brokers[broker]

	if bp == nil {
		bp = p.newBrokerProducer(broker)
		p.brokers[broker] = bp
		p.brokerRefs[bp] = 0
	}

	p.brokerRefs[bp]++

	return bp
}
```

又调用了`newBrokerProducer()`，继续追踪下去：

```go
func (p *asyncProducer) newBrokerProducer(broker *Broker) *brokerProducer {
	var (
		input     = make(chan *ProducerMessage)
		bridge    = make(chan *produceSet)
		responses = make(chan *brokerProducerResponse)
	)

	bp := &brokerProducer{
		parent:         p,
		broker:         broker,
		input:          input,
		output:         bridge,
		responses:      responses,
		stopchan:       make(chan struct{}),
		buffer:         newProduceSet(p),
		currentRetries: make(map[string]map[int32]error),
	}
	go withRecover(bp.run)

	// minimal bridge to make the network response `select`able
	go withRecover(func() {
		for set := range bridge {
			request := set.buildRequest()

			response, err := broker.Produce(request)

			responses <- &brokerProducerResponse{
				set: set,
				err: err,
				res: response,
			}
		}
		close(responses)
	})

	if p.conf.Producer.Retry.Max <= 0 {
		bp.abandoned = make(chan struct{})
	}

	return bp
}
```

这里又启动了两个 goroutine，一个为 run，一个是匿名函数姑且称为 bridge。

> bridge 看起来是真正的发送逻辑，那么 batch handle 逻辑应该是在 run 方法里了。

这里先分析 bridge 函数，run 在下一章分析。

##### buildRequest

buildRequest 方法主要是构建一个标准的 Kafka Request 消息。

> 根据不同版本、是否配置压缩信息做了额外处理，这里先忽略，只看核心代码：

```go
func (ps *produceSet) buildRequest() *ProduceRequest {	
	req := &ProduceRequest{
		RequiredAcks: ps.parent.conf.Producer.RequiredAcks,
		Timeout:      int32(ps.parent.conf.Producer.Timeout / time.Millisecond),
	}
	for topic, partitionSets := range ps.msgs {
		for partition, set := range partitionSets {
				rb := set.recordsToSend.RecordBatch
				if len(rb.Records) > 0 {
					rb.LastOffsetDelta = int32(len(rb.Records) - 1)
					for i, record := range rb.Records {
						record.OffsetDelta = int64(i)
					}
				}
				req.AddBatch(topic, partition, rb)
				continue
			}
    }
}
```

首先是构建一个 req 对象，然后遍历 ps.msg 中的消息，根据 topic 和 partition 分别写入到 req 中。

>  **Produce**

```go
func (b *Broker) Produce(request *ProduceRequest) (*ProduceResponse, error) {
	var (
		response *ProduceResponse
		err      error
	)

	if request.RequiredAcks == NoResponse {
		err = b.sendAndReceive(request, nil)
	} else {
		response = new(ProduceResponse)
		err = b.sendAndReceive(request, response)
	}

	if err != nil {
		return nil, err
	}

	return response, nil
}
```

最终调用了`sendAndReceive()`方法将消息发送出去。

如果我们设置了需要 Acks，就会传一个 response 进去接收返回值；如果没设置，那么消息发出去之后，就不管了。

```go
func (b *Broker) sendAndReceive(req protocolBody, res protocolBody) error {
    
	promise, err := b.send(req, res != nil, responseHeaderVersion)
	if err != nil {
		return err
	}
	select {
	case buf := <-promise.packets:
		return versionedDecode(buf, res, req.version())
	case err = <-promise.errors:
		return err
	}
}
```

最终通过`bytes, err := b.write(buf)` 发送出去。

```go
func (b *Broker) write(buf []byte) (n int, err error) {
	if err := b.conn.SetWriteDeadline(time.Now().Add(b.conf.Net.WriteTimeout)); err != nil {
		return 0, err
	}
	// 这里就是 net 包中的逻辑了。。
	return b.conn.Write(buf)
}
```

至此，`Sarama`生产者相关的内容就介绍完毕了。

> 还有一个比较重要的，消息打包批量发送的逻辑，比较多再下一章讲。

#### 消息打包源码分析

在之前 BrokerProducer 逻辑中启动了两个 goroutine，其中 bridge 从 chan 中取消息并真正发送出去。

*那么这个 chan 里的消息是哪里来的呢?*

其实这就是另一个 goroutine 的工作了。

```go
func (p *asyncProducer) newBrokerProducer(broker *Broker) *brokerProducer {
	var (
		input     = make(chan *ProducerMessage)
		bridge    = make(chan *produceSet)
		responses = make(chan *brokerProducerResponse)
	)

	bp := &brokerProducer{
		parent:         p,
		broker:         broker,
		input:          input,
		output:         bridge,
		responses:      responses,
		stopchan:       make(chan struct{}),
		buffer:         newProduceSet(p),
		currentRetries: make(map[string]map[int32]error),
	}
	go withRecover(bp.run)

	// minimal bridge to make the network response `select`able
	go withRecover(func() {
		for set := range bridge {
			request := set.buildRequest()

			response, err := broker.Produce(request)

			responses <- &brokerProducerResponse{
				set: set,
				err: err,
				res: response,
			}
		}
		close(responses)
	})

	if p.conf.Producer.Retry.Max <= 0 {
		bp.abandoned = make(chan struct{})
	}

	return bp
}
```

##### run

```go
func (bp *brokerProducer) run() {
	var output chan<- *produceSet

	for {
		select {
		case msg, ok := <-bp.input:
            // 1. 检查 buffer 空间是否足够存放当前 msg
			if bp.buffer.wouldOverflow(msg) {
				if err := bp.waitForSpace(msg, false); err != nil {
					bp.parent.retryMessage(msg, err)
					continue
				}
			}
			// 2. 将 msg 存入 buffer
			if err := bp.buffer.add(msg); err != nil {
				bp.parent.returnError(msg, err)
				continue
			}
            // 3. 如果间隔时间到了，也会将消息发出去
		case <-bp.timer:
			bp.timerFired = true
            // 4. 将 buffer 里的数据发送到 局部变量 output chan 里
		case output <- bp.buffer:
			bp.rollOver()
		case response, ok := <-bp.responses:
			if ok {
				bp.handleResponse(response)
			}
		} 
		// 5.如果发送时间到了 或者消息大小或者条数达到阈值 则表示可以发送了 将  bp.output chan 赋值给局部变量 output
		if bp.timerFired || bp.buffer.readyToFlush() {
			output = bp.output
		} else {
			output = nil
		}
	}
}
```

- 1）首先检测 buffer 空间
- 2）将 msg 写入 buffer
- 3）后面的 3 4 5 步都是在发送消息，或者为发送消息做准备

##### wouldOverflow

```go
if bp.buffer.wouldOverflow(msg) {
    if err := bp.waitForSpace(msg, false); err != nil {
        bp.parent.retryMessage(msg, err)
        continue
    }
}
```

在 add 之前先调用`bp.buffer.wouldOverflow(msg)` 方法检查 buffer 是否存在足够空间以存放当前消息。

wouldOverflow 比较简单，就是判断当前消息大小或者消息数量是否超过设定值：

```go
func (ps *produceSet) wouldOverflow(msg *ProducerMessage) bool {
	switch {
	case ps.bufferBytes+msg.byteSize(version) >= int(MaxRequestSize-(10*1024)):
		return true
	case ps.msgs[msg.Topic] != nil && ps.msgs[msg.Topic][msg.Partition] != nil &&
		ps.msgs[msg.Topic][msg.Partition].bufferBytes+msg.byteSize(version) >= ps.parent.conf.Producer.MaxMessageBytes:
		return true
	case ps.parent.conf.Producer.Flush.MaxMessages > 0 && ps.bufferCount >= ps.parent.conf.Producer.Flush.MaxMessages:
		return true
	default:
		return false
	}
}
```

如果不够就要调用`bp.waitForSpace()` 等待 buffer 腾出空间，其实就是把 buffer 里的消息发到 output chan。

> 这个 output chan 就是前面匿名函数里的 bridge。

```go
func (bp *brokerProducer) waitForSpace(msg *ProducerMessage, forceRollover bool) error {
	for {
		select {
		case response := <-bp.responses:
			bp.handleResponse(response)
			if reason := bp.needsRetry(msg); reason != nil {
				return reason
			} else if !bp.buffer.wouldOverflow(msg) && !forceRollover {
				return nil
			}
		case bp.output <- bp.buffer:
			bp.rollOver()
			return nil
		}
	}
}
```

##### add

接下来是调用`bp.buffer.add()`把消息添加到 buffer，功能比较简单，把待发送的消息添加到 buffer 中。

```go
func (ps *produceSet) add(msg *ProducerMessage) error {
		// 1.消息编码
		key, err = msg.Key.Encode()
		val, err = msg.Value.Encode()
		// 2.添加消息到 set.msgs 数组
		set.msgs = append(set.msgs, msg)
		// 3.添加到set.recordsToSend
		msgToSend := &Message{Codec: CompressionNone, Key: key, Value: val}
		if ps.parent.conf.Version.IsAtLeast(V0_10_0_0) {
			msgToSend.Timestamp = timestamp
			msgToSend.Version = 1
		}
		set.recordsToSend.MsgSet.addMessage(msgToSend)
		// 4. 增加 buffer 大小和 buffer 中的消息条数
		ps.bufferBytes += size
		ps.bufferCount++
}
```

`set.recordsToSend.MsgSet.addMessage`也很简单：

```go
func (ms *MessageSet) addMessage(msg *Message) {
	block := new(MessageBlock)
	block.Msg = msg
	ms.Messages = append(ms.Messages, block)
}
```

##### 定时发送

因为异步发送者除了消息数或者消息大小达到阈值会触发一次发送之外，到了一定时间也会触发一次发送，具体逻辑也在这个 run 方法里，这个地方比较有意思。

```go
func (bp *brokerProducer) run() {
	var output chan<- *produceSet
	for {
		select {
		case msg, ok := <-bp.input:
        // 1.时间到了就将 bp.timerFired 设置为 true
		case <-bp.timer:
			 bp.timerFired = true
        // 3.直接把 buffer 里的消息往局部变量 output 里发
		case output <- bp.buffer:
			bp.rollOver()
		}
		// 2.如果时间到了，或者 buffer 里的消息达到阈值后都会触发真正的发送逻辑，这里实现比较有意思，需要发送的时候就把 bp.output 也就是存放真正需要发送的批量消息的 chan 赋值给局部变量 output，如果不需要发送就把局部变量 output 清空
		if bp.timerFired || bp.buffer.readyToFlush() {
			output = bp.output
		} else {
			output = nil
		}
	}
}
```

根据注释中的 1、2、3步骤看来，如果第二步需要发送就会给 output 赋值，这样下一轮 select 的时候`case output <- bp.buffer:` 这个 case 就可能会执行到，就会把消息发给 output，实际上就是发送给了 bp.output.

如果第二步时不需要发消息，output 就被置空，select 时对应的 case 就不会被执行。

> 正常写法一般是在启动一个 goroutine 来处理定时发送的功能，但是这样两个 goroutine 之间就会存在竞争，会影响性能。这样处理省去了加解锁过程，性能会高一些，但是随之而来的是代码复杂度的提升。



**Consumer**

```go
/*
	本例展示最简单的 消费者组 的使用（除消费者组外 kafka 还有独立消费者）
	创建消费者时提供相同的 groupID 即可自动加入 groupID 对应的消费者组
	组中所有消费者以分区为单位，拆分被消费的topic
	例如: 该 topic 中有 10 个分区，该组中有两个消费者，那么每个消费者会消费 5 个分区。
	但是如果该 topic 只有 1 个分区那只能一个消费者能消费，另一个消费者一条消息都消费不到。
	因为是以分区为单位拆分的。
	名词: consumerGroup、partition、 claim 、session
*/

// sarama 库中消费者组为一个接口 sarama.ConsumerGroup 所有实现该接口的类型都能当做消费者组使用。
// MyConsumerGroupHandler 实现 sarama.ConsumerGroup 接口，作为自定义ConsumerGroup
type MyConsumerGroupHandler struct {
	name  string
	count int64
}

// Setup 执行在 获得新 session 后 的第一步, 在 ConsumeClaim() 之前
func (MyConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup 执行在 session 结束前, 当所有 ConsumeClaim goroutines 都退出时
func (MyConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 具体的消费逻辑
func (h MyConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			fmt.Printf("[consumer] name:%s topic:%q partition:%d offset:%d\n", h.name, msg.Topic, msg.Partition, msg.Offset)
			// 标记消息已被消费 内部会更新 consumer offset
			sess.MarkMessage(msg, "")
			h.count++
			if h.count%10000 == 0 {
				fmt.Printf("name:%s 消费数:%v\n", h.name, h.count)
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-sess.Context().Done():
			return nil
		}
	}
}

func ConsumerGroup(topic, group, name string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cg, err := sarama.NewConsumerGroup([]string{conf.HOST}, group, config)
	if err != nil {
		log.Fatal("NewConsumerGroup err: ", err)
	}
	defer cg.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler := MyConsumerGroupHandler{name: name}
		for {
			fmt.Println("running: ", name)
			/*
				![important]
				应该在一个无限循环中不停地调用 Consume()
				因为每次 Rebalance 后需要再次执行 Consume() 来恢复连接
				Consume 开始才发起 Join Group 请求 如果当前消费者加入后成为了 消费者组 leader,则还会进行 Rebalance 过程，从新分配
				组内每个消费组需要消费的 topic 和 partition，最后 Sync Group 后才开始消费
				具体信息见 https://github.com/lixd/kafka-go-example/issues/4
			*/
			err = cg.Consume(ctx, []string{topic}, handler)
			if err != nil {
				log.Println("Consume err: ", err)
			}
			// 如果 context 被 cancel 了，那么退出
			if ctx.Err() != nil {
				return
			}
		}
	}()
	wg.Wait()
}
```



