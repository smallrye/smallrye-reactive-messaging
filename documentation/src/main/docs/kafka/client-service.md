# KafkaClientService

For advanced use cases, SmallRye Reactive Messaging provides a bean of
type `KafkaClientService` that you can inject:

``` java
@Inject
KafkaClientService kafka;
```

From there, you can obtain an
`io.smallrye.reactive.messaging.kafka.KafkaProducer` and an
`io.smallrye.reactive.messaging.kafka.KafkaConsumer`.

`KafkaProducer` and `KafkaConsumer` expose a non-blocking API on top of
the Kafka client API. They also mediate access to the threads that
SmallRye Reactive Messaging uses to run all Kafka operations: the
*polling thread*, used for consuming records from Kafka topics, and the
*sending thread*, used for producing records to Kafka topics. (Just to
be clear: each *channel* has its own polling thread and sending thread.)

The reason why SmallRye Reactive Messaging uses a special thread to run
the poll loop should be obvious: the `Consumer` API is blocking. The
`Producer` API, on the other hand, is documented to be non-blocking.
However, in present versions, Kafka doesn’t guarantee that in all cases;
see [KAFKA-3539](https://issues.apache.org/jira/browse/KAFKA-3539) for
more details. That is why SmallRye Reactive Messaging uses a dedicated
thread to run the send operations as well.

Sometimes, SmallRye Reactive Messaging provides direct access to the
Kafka `Producer` or `Consumer`. For example, a
[`KafkaConsumerRebalanceListener`](consumer-rebalance-listener.md)
methods are always invoked on the polling thread, so they give you
direct access to `Consumer`. In such case, you should use the
`Producer`/`Consumer` API directly, instead of the
`KafkaProducer`/`KafkaConsumer` API.
