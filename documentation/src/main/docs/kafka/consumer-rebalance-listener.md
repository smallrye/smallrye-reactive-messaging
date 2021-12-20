# Consumer Rebalance Listener

To handle offset commit and assigned partitions yourself, you can
provide a consumer rebalance listener. To achieve this, implement the
`io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener`
interface, make the implementing class a bean, and add the `@Identifier`
qualifier. A usual use case is to store offset in a separate data store
to implement exactly-once semantic, or starting the processing at a
specific offset.

The listener is invoked every time the consumer topic/partition
assignment changes. For example, when the application starts, it invokes
the `partitionsAssigned` callback with the initial set of
topics/partitions associated with the consumer. If, later, this set
changes, it calls the `partitionsRevoked` and `partitionsAssigned`
callbacks again, so you can implement custom logic.

Note that the rebalance listener methods are called from the Kafka
*polling* thread and must block the caller thread until completion.
That’s because the rebalance protocol has synchronization barriers, and
using asynchronous code in a rebalance listener may be executed after
the synchronization barrier.

When topics/partitions are assigned or revoked from a consumer, it
pauses the message delivery and restarts once the rebalance completes.

If the rebalance listener handles offset commit on behalf of the user
(using the `ignore` commit strategy), the rebalance listener **must**
commit the offset synchronously in the `partitionsRevoked` callback. We
also recommend applying the same logic when the application stops.

Unlike the `ConsumerRebalanceListener` from Apache Kafka, the
`io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener`
methods pass the Kafka `Consumer` and the set of topics/partitions.

## Example

In this example we set-up a consumer that always starts on messages from
at most 10 minutes ago (or offset 0). First we need to provide a bean
that implements the
`io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener`
interface and is annotated with `@Identifier`. We then must configure
our inbound connector to use this named bean.

``` java
{{ insert('kafka/inbound/KafkaRebalancedConsumerRebalanceListener.java') }}
```

``` java
{{ insert('kafka/inbound/KafkaRebalancedConsumer.java') }}
```

To configure the inbound connector to use the provided listener we
either set the consumer rebalance listener’s name:

-   `mp.messaging.incoming.rebalanced-example.consumer-rebalance-listener.name=rebalanced-example.rebalancer`

Or have the listener’s name be the same as the group id:

-   `mp.messaging.incoming.rebalanced-example.group.id=rebalanced-example.rebalancer`

Setting the consumer rebalance listener’s name takes precedence over
using the group id.
