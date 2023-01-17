# Receiving Kafka Records

The Kafka Connector retrieves Kafka Records from Kafka Brokers and maps
each of them to Reactive Messaging `Messages`.

## Example

Let’s imagine you have a Kafka broker running, and accessible using the
`kafka:9092` address (by default it would use `localhost:9092`).
Configure your application to receive Kafka records from a Kafka *topic*
on the `prices` channel as follows:

```properties
kafka.bootstrap.servers=kafka:9092 # <1>

mp.messaging.incoming.prices.connector=smallrye-kafka # <2>
mp.messaging.incoming.prices.value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer # <3>
mp.messaging.incoming.prices.broadcast=true # <4>
```

1.  Configure the broker location. You can configure it globally or per
    channel

2.  Configure the connector to manage the `prices` channel

3.  Sets the (Kafka) deserializer to read the record’s value

4.  Make sure that we can receive from more than one consumer (see
    `KafkaPriceConsumer` and `KafkaPriceMessageConsumer` below)

!!!note
    You don’t need to set the Kafka topic. By default, it uses the channel
    name (`prices`). You can configure the `topic` attribute to override it.

Then, your application receives `Message<Double>`. You can consume the
payload directly:

``` java
{{ insert('kafka/inbound/KafkaPriceConsumer.java') }}
```

Or, you can retrieve the `Message<Double>`:

``` java
{{ insert('kafka/inbound/KafkaPriceMessageConsumer.java') }}
```

## Deserialization

The deserialization is handled by the underlying Kafka Client. You need
to configure the:

-   `mp.messaging.incoming.[channel-name].value.deserializer` to
    configure the value deserializer (mandatory)

-   `mp.messaging.incoming.[channel-name].key.deserializer` to configure
    the key deserializer (optional, default to `String`)

If you want to use a custom deserializer, add it to your `CLASSPATH` and
configure the associate attribute.

In addition, the Kafka Connector also provides a set of *message
converters*. So you can receive *payloads* representing records from
Kafka using:

-   {{ javadoc('io.smallrye.reactive.messaging.kafka.Record', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }} - a pair key/value
-   [ConsumerRecord<K,V>](https://kafka.apache.org/26/javadoc/index.html?org/apache/kafka/clients/consumer/ConsumerRecord.html) -
    a structure representing the record with all its metadata

``` java
{{ insert('kafka/inbound/Converters.java', 'code') }}
```

## Inbound Metadata

Messages coming from Kafka contains an instance of
{{ javadoc('io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }}
in the metadata. It provides the key, topic, partitions,
headers and so on:

``` java
{{ insert('kafka/inbound/KafkaMetadataExample.java', 'code') }}
```

## Acknowledgement

When a message produced from a Kafka record is acknowledged, the
connector invokes a *commit strategy*. These strategies decide when the
consumer offset for a specific topic/partition is committed. Committing
an offset indicates that all previous records have been processed. It is
also the position where the application would restart the processing
after a crash recovery or a restart.

Committing every offset has performance penalties as Kafka offset
management can be slow. However, not committing the offset often enough
may lead to message duplication if the application crashes between two
commits.

The Kafka connector supports three strategies:

-   `throttled` keeps track of received messages and commit to the next
    offset after the latest *acked* message in sequence. This strategy
    guarantees *at-least-once delivery* even if the channel performs
    asynchronous processing. The connector tracks the received records
    and periodically (period specified by `auto.commit.interval.ms`
    (default: 5000)) commits the highest consecutive offset. The
    connector will be marked as unhealthy if a message associated with a
    record is not acknowledged in
    `throttled.unprocessed-record-max-age.ms` (default: 60000). Indeed,
    this strategy cannot commit the offset as soon as a single record
    processing fails (see failure-strategy to configure what happens on
    failing processing). If `throttled.unprocessed-record-max-age.ms` is
    set to less than or equal to 0, it does not perform any health check
    verification. Such a setting might lead to running out of memory if
    there are poison pill messages. This strategy is the default if
    `enable.auto.commit` is not explicitly set to `true`.

-   `checkpoint` allows persisting consumer offsets on a "state store",
    instead of committing them back to the Kafka broker. Using the
    `CheckpointMetadata` API, consumer code can persist a processing
    state with the offset to mark the progress of a consumer.
    When the processing continues from a previously persisted offset,
    it seeks the Kafka consumer to that offset and also restores the
    persisted state, continuing the stateful processing from where it
    left off. The `checkpoint` strategy holds locally the processing
    state associated with the latest offset, and persists it
    periodically to the state store (period specified by
    `auto.commit.interval.ms` (default: 5000)). The connector will be
    marked as unhealthy if no processing state is persisted to the state
    store in `checkpoint.unsynced-state-max-age.ms` (default: 10000).
    Using the `CheckpointMetadata` API the user code can force to persist
    the state on message ack. If `checkpoint.unsynced-state-max-age.ms`
    is set to less than or equal to 0, it does not perform any health
    check verification. For more information, see
    [Stateful processing with Checkpointing](#stateful-processing-with-checkpointing)

-   `latest` commits the record offset received by the Kafka consumer as
    soon as the associated message is acknowledged (if the offset is
    higher than the previously committed offset). This strategy provides
    *at-least-once* delivery if the channel processes the message
    without performing any asynchronous processing. This strategy should
    not be used on high-load as offset commit is expensive. However, it
    reduces the risk of duplicates.

-   `ignore` performs no commit. This strategy is the default strategy
    when the consumer is explicitly configured with `enable.auto.commit`
    to `true`. It delegates the offset commit to the Kafka client. When
    `enable.auto.commit` is `true` this strategy **DOES NOT** guarantee
    at-least-once delivery. However, if the processing failed between
    two commits, messages received after the commit and before the
    failure will be re-processed.

!!!important
    The Kafka connector disables the Kafka *auto commit* if not explicitly
    enabled. This behavior differs from the traditional Kafka consumer.

If high-throughout is important for you, and not limited by the
downstream, we recommend to either:

-   Use the `throttled` policy
-   or set `enable.auto.commit` to `true` and annotate the consuming
    method with `@Acknowledgment(Acknowledgment.Strategy.NONE)`

## Failure Management

If a message produced from a Kafka record is *nacked*, a failure
strategy is applied. The Kafka connector supports 3 strategies:

-   `fail` - fail the application, no more records will be processed.
    (default) The offset of the record that has not been processed
    correctly is not committed.

-   `ignore` - the failure is logged, but the processing continue. The
    offset of the record that has not been processed correctly is
    committed.

-   `dead-letter-queue` - the offset of the record that has not been
    processed correctly is committed, but the record is written to a
    (Kafka) *dead letter queue* topic.

-   `delayed-retry-topic` - the offset of the record that has not been
    processed correctly is still committed, but the record is written to
    a series of Kafka topics for retrying the processing with some delay.
    This allows retrying failed records by reconsuming them later without
    blocking the processing of the latest records.

The strategy is selected using the `failure-strategy` attribute.

### Dead Letter Queue

In the case of `dead-letter-queue`, you can configure the following
attributes:

- `dead-letter-queue.topic`: the topic to use to write the records not
    processed correctly, default is `dead-letter-topic-$channel`, with
    `$channel` being the name of the channel.
- `dead-letter-queue.producer-client-id`: the client id used by the kafka
producer when sending records to dead letter queue topic. If not specified
it will default to `kafka-dead-letter-topic-producer-$client-id`, with $client-id
being the value obtained from consumer client id.


- `dead-letter-queue.key.serializer`: the serializer used to write the
    record key on the dead letter queue. By default, it deduces the
    serializer from the key deserializer.

- `dead-letter-queue.value.serializer`: the serializer used to write
    the record value on the dead letter queue. By default, it deduces
    the serializer from the value deserializer.


The record written on the dead letter topic contains the original
record’s headers, as well as a set of additional headers about the
original record:

-   `dead-letter-reason` - the reason of the failure (the `Throwable`
    passed to `nack()`)

-   `dead-letter-cause` - the cause of the failure (the `getCause()` of
    the `Throwable` passed to `nack()`), if any

-   `dead-letter-topic` - the original topic of the record

-   `dead-letter-partition` - the original partition of the record
    (integer mapped to String)

-   `dead-letter-offset` - the original offset of the record (long
    mapped to String)

When using `dead-letter-queue`, it is also possible to change some
metadata of the record that is sent to the dead letter topic. To do
that, use the `Message.nack(Throwable, Metadata)` method:

``` java
{{ insert('kafka/inbound/KafkaDeadLetterExample.java', 'code') }}
```

The `Metadata` may contain an instance of `OutgoingKafkaRecordMetadata`.
If the instance is present, the following properties will be used:

-   key; if not present, the original record’s key will be used

-   topic; if not present, the configured dead letter topic will be used

-   partition; if not present, partition will be assigned automatically

-   headers; combined with the original record’s headers, as well as the
    `dead-letter-*` headers described above

### Delayed Retry Topic

!!!Experimental
    Delayed retry topic feature is experimental.

The delayed retry topic strategy allows failed records to be automatically retried by forwarding them to a series of retry topics.
Each retry topic is associated with a specific delay time, which is expressed in milliseconds.
When a record processing fails, it is forwarded to the first retry topic.
The failure strategy then consumes these records and dispatches them to be retried again once the delay time of the topic has elapsed.

If the processing of a record fails again, the message is forwarded to the next topic in the list, with possibly a longer delay time.
If the processing of a record keeps failing, it will eventually be abandoned.
Alternatively, if the `dead-letter-queue.topic` property is configured, the record will be sent to the dead letter queue.

The Kafka producer client used when forwarding records to retry topics can be configured using the *dead-letter-queue* properties
namely, `dead-letter-queue.producer-client-id`, `dead-letter-queue.key.serializer` and `dead-letter-queue.value.serializer`.

Delayed retry topics and delays can be configured with following attributes:

- `delayed-retry-topic.topics` :
The comma-separated list of retry topics, each one suffixed with `_[DELAY_IN_MILLISECONDS]` for indicating the delay time.
For example, `my_retry_topic_2000,my_retry_topic_4000,my_retry_topic_10000` will use three topics
*my_retry_topic_2000*, *my_retry_topic_4000* and *my_retry_topic_10000*, with 2000ms 4000ms and 10000ms respectively.

    If not configured the source channel name is used, with 10, 20 and 50 seconds of delay,
    ex. for a channel named `source`, retry topics will be `source_retry_10000`, `source_retry_20000`, `source_retry_50000`.

- `delayed-retry-topic.max-retries` :
The maximum number of retries before abandoning the retries.
If configured higher than the number of retry topics the last topic is used until maximum number of retries is reached.
This can be configured to use a single retry topic with a fixed delay and multiple retries.

    For example, `delayed-retry-topic.topics=source_retry_10000` and `delayed-retry-topic.max-retries=4` will forward failed records
    to the topic *source_retry_10000* with maximum of 4 retries.

- `delayed-retry-topic.timeout` :
The global timeout in milliseconds for a retried record.
The timeout is calculated from the first failure for a record.
If the next retry will reach the timeout, instead of forwarding to the retry topic the retry is abandoned and,
if configured, the record is forwarded to the dead letter queue.

    The default is 120 seconds.

!!!Important
    While you can use [Smallrye Fault Tolerance to retry processing](#retrying-processing),
    it will block the processing of further messages until the retried record is processed successfully, or abandoned.

    Delayed retry topic failure strategy allows effectively implementing non-blocking retries.
    But it will not preserve the order of messages inside a topic-partition.

The record written on the delayed retry topics will preserve the key and partition of the original record.
It also contains the original record’s headers, as well as a set of additional headers about the original record:

- `delayed-retry-count` the current number of retries
- `delayed-retry-original-timestamp` the original timestamp of the record
- `delayed-retry-first-processing-timestamp` the first processing timestamp of the record
- `delayed-retry-reason` the reason of the failure (the `Throwable` passed to `nack()`)
- `delayed-retry-cause` the cause of the failure (the `getCause()` of the `Throwable` passed to `nack()`), if any
- `delayed-retry-topic` the original topic of the record
- `delayed-retry-partition` the original partition of the record
- `delayed-retry-offset` the original offset of the record
- `delayed-retry-exception-class-name` the class name of the throwable passed to `nack()`
- `delayed-retry-cause-class-name` the class name of the the `getCause()` of the `Throwable` passed to `nack()`, if any

As for the dead letter queue it is possible to change forwarded values by providing a `OutgoingKafkaRecordMetadata`
when the message is nacked using `Message.nack(Throwable, Metadata)`.

!!! Note "Multiple partitions"
    The delayed retry topic strategy does not create retry topics automatically.
    If the source topic has multiple partitions, delayed retry and dead letter queue topics would need to be setup with the same number of partitions.

    It is possible to scale consumer application instances according to the number of partitions.
    But it is not guaranteed that the retry topics consumer will be assigned the same partition(s) as the main topic consumer.
    Therefore, retry processing of a record can happen in an other instance.

## Custom commit and failure strategies

In addition to provided strategies, it is possible to implement custom
commit and failure strategies and configure Kafka channels with them.

For example, for a custom commit strategy, implement the
{{ javadoc('io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }} interface,
and provide a managed bean implementing the `KafkaCommitHandler.Factory` interface,
identified using `@Identifier` qualifier.

``` java
{{ insert('kafka/inbound/KafkaCustomCommit.java') }}
```

Finally, to use the custom commit strategy,
set the `commit-strategy` attribute to the identifier of the commit handler factory:
`mp.messaging.incoming.$channel.commit-strategy=custom`.
Similarly, custom failure strategies can be configured using `failure-strategy` attribute.

!!!note
    If the custom strategy implementation inherits
    {{ javadoc('io.smallrye.reactive.messaging.kafka.commit.ContextHolder', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }} class it can access the
    Vert.x event-loop context created for the Kafka consumer


## Retrying processing

You can combine Reactive Messaging with [SmallRye Fault
Tolerance](https://github.com/smallrye/smallrye-fault-tolerance), and
retry processing when it fails:

``` java
@Incoming("kafka")
@Outgoing("processed")
@Retry(delay = 10, maxRetries = 5)
public String process(String v) {
   // ... retry if this method throws an exception
}
```

You can configure the delay, the number of retries, the jitter...

If your method returns a `Uni`, you need to add the `@NonBlocking`
annotation:

``` java
@Incoming("kafka")
@Outgoing("processed")
@Retry(delay = 10, maxRetries = 5)
@NonBlocking
public Uni<String> process(String v) {
   // ... retry if this method throws an exception or the returned Uni produce a failure
}
```

The incoming messages are acknowledged only once the processing
completes successfully. So, it commits the offset after the successful
processing. If after the retries the processing still failed, the
message is *nacked* and the failure strategy is applied.

You can also use `@Retry` on methods only consuming incoming messages:

``` java
@Incoming("kafka")
@Retry(delay = 10, maxRetries = 5)
public void consume(String v) {
   // ... retry if this method throws an exception
}
```

## Handling deserialization failures

Because deserialization happens before creating a `Message`, the failure
strategy presented above cannot be applied. However, when a
deserialization failure occurs, you can intercept it and provide a
fallback value. To achieve this, create a CDI bean implementing the
{{ javadoc('io.smallrye.reactive.messaging.kafka.api.DeserializationFailureHandler', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }}
interface:

``` java
@ApplicationScoped
@Identifier("failure-retry") // Set the name of the failure handler
public class MyDeserializationFailureHandler
    implements DeserializationFailureHandler<JsonObject> { // Specify the expected type

    @Override
    public JsonObject decorateDeserialization(Uni<JsonObject> deserialization,
            String topic, boolean isKey, String deserializer, byte[] data,
            Headers headers) {
        return deserialization
                    .onFailure().retry().atMost(3)
                    .await().atMost(Duration.ofMillis(200));
    }
}
```

The bean must be exposed with the `@Identifier` qualifier specifying the
name of the bean. Then, in the connector configuration, specify the
following attribute:

-   `mp.messaging.incoming.$channel.key-deserialization-failure-handler`:
    name of the bean handling deserialization failures happening for the
    record’s key

-   `mp.messaging.incoming.$channel.value-deserialization-failure-handler`:
    name of the bean handling deserialization failures happening for the
    record’s value,

The handler is called with the deserialization action as a `Uni<T>`, the
record’s topic, a boolean indicating whether the failure happened on a
key, the class name of the deserializer that throws the exception, the
corrupted data, the exception, and the records headers augmented with
headers describing the failure (which ease the write to a dead letter).
On the deserialization `Uni` failure strategies like retry, providing a
fallback value or applying timeout can be implemented. Note that the
method must await on the result and return the deserialized object.
Alternatively, the handler can only implement
`handleDeserializationFailure` method and provide a fallback value,
which may be `null`.

If you don’t configure a deserialization failure handlers and a
deserialization failure happens, the application is marked unhealthy.
You can also ignore the failure, which will log the exception and
produce a `null` value. To enable this behavior, set the
`mp.messaging.incoming.$channel.fail-on-deserialization-failure`
attribute to `false`.

## Receiving Cloud Events

The Kafka connector supports [Cloud Events](https://cloudevents.io/).
When the connector detects a *structured* or *binary* Cloud Events, it
adds a {{ javadoc('io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }} in the metadata of the
Message. `IncomingKafkaCloudEventMetadata` contains the various (mandatory and optional) Cloud Event attributes.

If the connector cannot extract the Cloud Event metadata, it sends the
Message without the metadata.

### Binary Cloud Events

For `binary` Cloud Events, **all** mandatory Cloud Event attributes must
be set in the record header, prefixed by `ce_` (as mandated by the
[protocol
binding](https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md)).
The connector considers headers starting with the `ce_` prefix but not
listed in the specification as extensions. You can access them using the
`getExtension` method from `IncomingKafkaCloudEventMetadata`. You can
retrieve them as `String`.

The `datacontenttype` attribute is mapped to the `content-type` header
of the record. The `partitionkey` attribute is mapped to the record’s
key, if any.

Note that all headers are read as UTF-8.

With binary Cloud Events, the record’s key and value can use any
deserializer.

### Structured Cloud Events

For `structured` Cloud Events, the event is encoded in the record’s
value. Only JSON is supported, so your event must be encoded as JSON in
the record’s value.

Structured Cloud Event must set the `content-type` header of the record
to `application/cloudevents` or prefix the value with
`application/cloudevents` such as:
`application/cloudevents+json; charset=UTF-8`.

To receive structured Cloud Events, your value deserializer must be:

-   `org.apache.kafka.common.serialization.StringDeserializer`

-   `org.apache.kafka.common.serialization.ByteArrayDeserializer`

-   `io.vertx.kafka.client.serialization.JsonObjectDeserializer`

As mentioned previously, the value must be a valid JSON object
containing at least all the mandatory Cloud Events attributes.

If the record is a structured Cloud Event, the created Message’s payload
is the Cloud Event `data`.

The `partitionkey` attribute is mapped to the record’s key if any.

## Consumer Rebalance Listener

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

### Example

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

## Receiving Kafka Records in Batches

By default, incoming methods receive each Kafka record individually.
Under the hood, Kafka consumer clients poll the broker constantly and
receive records in batches, presented inside the `ConsumerRecords`
container.

In **batch** mode, your application can receive all the records returned
by the consumer **poll** in one go.

To achieve this you need to set
`mp.messaging.incoming.$channel.batch=true` **and** specify a compatible
container type to receive all the data:

``` java
{{ insert('kafka/inbound/KafkaRecordBatchPayloadExample.java', 'code') }}
```

The incoming method can also receive `Message<List<Payload>`,
`KafkaBatchRecords<Payload>` `ConsumerRecords<Key, Payload>` types, They
give access to record details such as offset or timestamp :

``` java
{{ insert('kafka/inbound/KafkaRecordBatchExample.java', 'code') }}
```

Note that the successful processing of the incoming record batch will
commit the latest offsets for each partition received inside the batch.
The configured commit strategy will be applied for these records only.

Conversely, if the processing throws an exception, all messages are
*nacked*, applying the failure strategy for all the records inside the
batch.

## Stateful processing with Checkpointing

!!!warning "Experimental"
    Checkpointing is experimental, and APIs and features are subject
    to change in the future.

The `checkpoint` commit strategy allows for a Kafka incoming channel to
manage topic-partition offsets, not by committing on the Kafka broker,
but by persisting consumers' advancement on a
[_state store_](#implementing-state-stores).

In addition to that, if the consumer builds an internal state as
a result of consumed records, the topic-partition offset persisted
to the state store can be associated with a _processing state_,
saving the local state to the persistent store. When a consumer
restarts or consumer group instances scale, i.e. when new partitions
get assigned to the consumer, the checkpointing works by resuming the
processing from the latest offset and its saved state.

The `@Incoming` channel consumer code can manipulate the processing
state through the `CheckpointMetadata` API:

``` java
{{ insert('kafka/inbound/KafkaCheckpointExample.java', 'code') }}
```

The `transform` method allows applying a transformation function to
the current state, producing a changed state and registering it
locally for checkpointing. By default, the local state is synced
(persisted) to the state store periodically, period specified by
`auto.commit.interval.ms`, (default: 5000). If `persistOnAck` flag
is given, the latest state is persisted to the state store eagerly
on message acknowledgment. The `setNext` method works similarly
directly setting the latest state.

The `checkpoint` commit strategy tracks when a processing state
is last persisted for each topic-partition. If an outstanding state
change can not be persisted for `checkpoint.unsynced-state-max-age.ms`
(default: 10000), the channel is marked unhealthy.

Where and how processing states are persisted is decided by the
state store implementation. This can be configured on the incoming
channel using `checkpoint.state-store` configuration property,
using the state store factory identifier name.
The serialization of state objects depends on the state store
implementation. In order to instruct state stores for serialization
can require configuring the type name of state objects
using `checkpoint.state-type` property.

In order to keep Smallrye Reactive Messaging free of persistence-related
dependencies, this library includes only a default state store named `file`.
It is based on Vert.x Filesystem API and stores the processing state
in Json formatted files, in a local directory configured by the
`checkpoint.file.state-dir` property. State files follow the naming
scheme `[consumer-group-id]:[topic]:[partition]`.

### Implementing State Stores

State store implementations are required to implement `CheckpointStateStore`
interface, and provide a managed bean implementing
`CheckpointStateStore.Factory`, identified with `@Identifier` bean
qualifier indicating the name of the state-store.
The factory bean identifier indicates the name to configure on
`checkpoint.state-store` config property.
The factory is discovered as a CDI managed bean and state store is
created once per channel:

``` java
{{ insert('kafka/inbound/MyCheckpointStateStore.java') }}
```

The checkpoint commit strategy calls the state store in following events:

- `fetchProcessingState` : on partitions assigned, to seek the consumer to the latest offset.
- `persistProcessingState` on partitions revoked, to persist the state of last processed record.
- `persistProcessingState` on message acknowledgement, if a new state is set during the processing and `persistOnAck` flag is set.
- `persistProcessingState` on `auto.commit.interval.ms` intervals, if a new state is set during processing.
- `persistProcessingState` on channel shutdown.
- `close` on channel shutdown.


## Configuration Reference

{{ insert('../../../target/connectors/smallrye-kafka-incoming.md') }}

You can also pass any property supported by the underlying [Kafka
consumer](https://kafka.apache.org/documentation/#consumerconfigs).

For example, to configure the `max.poll.records` property, use:

``` properties
mp.messaging.incoming.[channel].max.poll.records=1000
```

Some consumer client properties are configured to sensible default
values:

If not set, `reconnect.backoff.max.ms` is set to `10000` to avoid high
load on disconnection.

If not set, `key.deserializer` is set to
`org.apache.kafka.common.serialization.StringDeserializer`.

The consumer `client.id` is configured according to the number of
clients to create using `mp.messaging.incoming.[channel].partitions`
property.

-   If a `client.id` is provided, it is used as-is or suffixed with
    client index if `partitions` property is set.

-   If a `client.id` is not provided, it is generated as
    `kafka-consumer-[channel][-index]`.
