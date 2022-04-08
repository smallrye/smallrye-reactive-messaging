# Kafka Transactions and Exactly-Once Processing

!!!warning "Experimental"
    Kafka Transactions is an experimental feature.

[Kafka transactions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) enable atomic writes to multiple Kafka topics and partitions.
Inside a transaction, a producer writes records to the Kafka topic partitions as it would normally do.
If the transaction completes successfully, all the records previously written to the broker inside that transaction will be _committed_, and will be readable for consumers.
If an error during the transaction causes it to be _aborted_, none will be readable for consumers.

There are a couple of fundamental things to consider before using transactions:

* Each transactional producer is configured with a unique identifier called the `transactional.id`.
This is used to identify the same producer instance across application restarts.
By default, it is not configured, and transactions cannot be used.
When it is configured, the producer is limited to idempotent delivery, therefore `enable.idempotence=true` is implied.

* For __only__ reading transactional messages, a consumer needs to explicitly configure its `isolation.level` property to `read_committed`.
This will make sure that the consumer will deliver only records committed inside a transaction, and filter out messages from aborted transactions.

* It should also be noted that this does not mean all records atomically written inside a transaction will be read atomically by the consumer.
Transactional producers can write to multiple topics and partitions inside a transaction, but consumers do not know where the transaction starts or ends.
Not only multiple consumers inside a consumer group can share those partitions,
all records which were part of a single transaction can be consumed in different batch of records by a consumer.


Kafka connector provides `KafkaTransactions` custom emitter for writing records inside a transaction.
Before using a transaction we need to make sure that `transactional.id` is configured on the channel properties.

    mp.messaging.outgoing.tx-out-example.transactional.id=example-tx-producer

It can be used as a regular emitter `@Channel`:

``` java
{{ insert('kafka/outbound/KafkaTransactionalProducer.java') }}
```

!!!note
    When `transactional.id` is provided `KafkaProducer#initTransactions` is called when the underlying Kafka producer is created.

The function given to the `withTransaction` method receives a `TransactionalEmitter` for producing records, and returns a `Uni` that provides the result of the transaction.
If the processing completes successfully, the producer is flushed and the transaction is committed.
If the processing throws an exception, returns a failing `Uni`, or marks the `TransactionalEmitter` for abort, the transaction is aborted.

If this method is called on a Vert.x context, the processing function is also called on that context.
Otherwise, it is called on the sending thread of the producer.

!!!important
    A transaction is considered _in progress_ from the call to the `withTransaction` until the returned `Uni` results in success or failure.
    While a transaction is in progress, subsequent calls to the `withTransaction`, including nested ones inside the given function, will throw `IllegalStateException`.
    Note that in Reactive Messaging, the execution of processing methods is already serialized, unless `@Blocking(ordered = false)` is used.
    If `withTransaction` can be called concurrently, for example from a REST endpoint, it is recommended to limit the concurrency of the execution.
    This can be done using the `@Bulkhead` annotation from Microprofile Fault Tolerance.

## Exactly-Once Processing

Kafka Transactions API also allows managing consumer offsets inside a transaction, together with produced messages.
This in turn enables coupling a consumer with a transactional producer in a consume-transform-produce pattern,
also known as exactly-once processing.
It means that an application consumes messages from a topic-partition, processes them, publishes the results to a topic-partition,
and commits offsets of the consumed messages in a transaction.

The `KafkaTransactions` emitter also provides a way to apply exactly-once processing to an incoming Kafka message inside a transaction.

The following example includes a batch of Kafka records inside a transaction.

    mp.messaging.outgoing.tx-out-example.transactional.id=example-tx-producer
    mp.messaging.outgoing.in-channel.batch=true
    mp.messaging.outgoing.in-channel.commit-strategy=ignore
    mp.messaging.outgoing.in-channel.failure-strategy=ignore

``` java
{{ insert('kafka/outbound/KafkaExactlyOnceProcessor.java') }}
```

If the processing completes successfully, before committing the transaction, the topic partition offsets of the given batch message will be committed to the transaction.

If the processing needs to abort, after aborting the transaction, the consumer's position is reset to the last committed offset, effectively resuming the consumption from that offset.
If no consumer offset has been committed, the consumer's position is reset to the beginning of the topic, even if the offset reset policy is `latest`.

!!!important
    When using exactly-once processing, consumed message offset commits are handled by the transaction and therefore `commit-strategy` needs to be `ignore`.

Any strategy can be employed for the `failure-strategy`.
Note that the `Uni` returned from the `#withTransaction` will yield a failure if the transaction fails and is aborted.

The application can choose to handle the error case, but for the message consumption to continue, `Uni` returned from the `@Incoming` method must not result in failure.
`KafkaTransactions#withTransactionAndAck` method will ack and nack the message but will not stop the reactive stream.
Ignoring the failure simply resets the consumer to the last committed offsets and resumes the processing from there.

!!!note
    It is recommended to use exactly-once processing along with the batch consumption mode.
    While it is possible to use it with a single Kafka message, it'll have a significant performance impact.
