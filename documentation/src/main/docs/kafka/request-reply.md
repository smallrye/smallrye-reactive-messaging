# Kafka Request/Reply

!!!warning "Experimental"
    Kafka Request Reply Emitter is an experimental feature.

The Kafka [Request-Reply](https://www.enterpriseintegrationpatterns.com/patterns/messaging/RequestReply.html) pattern allows you to publish a message to a Kafka topic and then await for a reply message that responds to the initial request.

The `KafkaRequestReply` emitter implements the requestor (or the client) of the request-reply pattern for Kafka outbound channels:

``` java
{{ insert('kafka/outbound/KafkaRequestReplyEmitter.java') }}
```

The `request` method publishes the request record to the configured target topic of the outgoing channel,
and polls a reply topic (by default, the target topic with `-replies` suffix) for a reply record.
When the reply is received the returned `Uni` is completed with the record value.

The request send operation generates a correlation id and sets a header (by default `REPLY_CORRELATION_ID`),
which it expects to be sent back in the reply record. Two additional headers are set on the request record:

- The topic from which the reply is expected, by default `REPLY_TOPIC` header,
which can be configured using the `reply.topic` channel attribute.
- Optionally, the partition from which the reply is expected, by default `REPLY_PARTITION` header.
The reply partition header is added only when the Kafka request reply is configured specifically to receive records from a topic-partition
, using the `reply.partition` channel attribute.
The reply partition header integer value is encoded in 4 bytes,
and helper methods `KafkaRequestReply#replyPartitionFromBytes` and `KafkaRequestReply#replyPartitionToBytes` can be used for custom operations.

The replier (or the server) can be implemented using a Reactive Messaging processor:

``` java
{{ insert('kafka/outbound/KafkaReplier.java') }}
```

Kafka outgoing channels detect default `REPLY_CORRELATION_ID`, `REPLY_TOPIC` and `REPLY_PARTITION` headers
and send the reply record to the expected topic-partition by propagating back the correlation id header.

Default headers can be configured, using `reply.correlation-id.header`, `reply.topic.header` and `reply.partition.header` channel attributes.
If custom headers are used the reply server needs some more manual work.
Given the following request/reply outgoing configuration:

```properties
mp.messaging.outgoing.reqrep.topic=requests
mp.messaging.outgoing.reqrep.reply.correlation-id.header=MY_CORRELATION
mp.messaging.outgoing.reqrep.reply.topic.header=MY_TOPIC
mp.messaging.outgoing.reqrep.reply.partition.header=MY_PARTITION
mp.messaging.incoming.request.topic=requests
```

The reply server can be implemented as the following:

``` java
{{ insert('kafka/outbound/KafkaCustomHeaderReplier.java') }}
```

## Requesting with `Message` types

Like the core Emitter's `send` methods, `request` method also can receive a `Message` type and return a message:

``` java
{{ insert('kafka/outbound/KafkaRequestReplyMessageEmitter.java') }}
```

!!! note
    The ingested reply type of the `KafkaRequestReply` is discovered at runtime,
    in order to configure a `MessageConveter` to be applied on the incoming message before returning the `Uni` result.

## Scaling Request/Reply

If multiple requestor instances are configured on the same outgoing topic, and the same reply topic,
each requestor consumer will generate a unique consumer group.id and
therefore all requestor instances will receive replies of all instances. If an observed correlation id doesn't match
the id of any pending replies, the reply is simply discarded.
With the additional network traffic this allows scaling requestors, (and repliers) dynamically.

Alternatively, requestor instances can be configured to consume replies from dedicated topics using `reply.topic` attribute,
or distinct partitions of a single topic, using `reply.partition` attribute.
The later will configure the Kafka consumer to assign statically to the given partition.

## Pending replies and reply timeout

By default, the `Uni` returned from the `request` method is configured to fail with timeout exception if no replies is received after 5 seconds.
This timeout is configurable with the channel attribute `reply.timeout`.

A snapshot of the list of pending replies is available through the `KafkaRequestReply#getPendingReplies` method.

## Waiting for topic-partition assignment

The requestor can be found in a position where a request is sent, and it's reply is already published to the reply topic,
before the requestor starts and polls the consumer.
In case the reply consumer is configured with `auto.offset.reset=latest`, which is the default value, this can lead to the requestor missing replies.

If `auto.offset.reset` is `latest`, at wiring time, before any request can take place, the `KafkaRequestReply`
finds partitions that the consumer needs to subscribe and waits for their assignment to the consumer.
The timeout of the initial subscription can be adjusted with `reply.initial-assignment-timeout` which defaults to the `reply.timeout`.
If this timeout fails, `KafkaRequestReply` will enter an invalid state which will require it to be restarted.
If set to `-1`, the `KafkaRequestReply` will not wait for the initial assignment of the reply consumer to sent requests.

On other occasions the `KafkaRequestReply#waitForAssignments` method can be used.

## Correlation Ids

The Kafka Request/Reply allows configuring the correlation id mechanism completely through a `CorrelationIdHandler` implementation.
The default handler is based on randomly generated UUID strings, written to byte array in Kafka record headers.
The correlation id handler implementation can be configured using the `reply.correlation-id.handler` attribute.
As mentioned the default configuration is `uuid`,
and an alternative `bytes` implementation can be used to generate 12 bytes random correlation ids.

Custom handlers can be implemented by proposing a CDI-managed bean with `@Identifier` qualifier.

## Reply Error Handling

If the reply server produces an error and can or would like to propagate the error back to the requestor, failing the returned `Uni`.

If configured using the `reply.failure.handler` channel attribute,
the `ReplyFailureHandler` implementations are discovered through CDI, matching the `@Identifier` qualifier.

A sample reply error handler can lookup header values and return the error to be thrown by the reply:

``` java
{{ insert('kafka/outbound/MyReplyFailureHandler.java') }}
```

`null` return value indicates that no error has been found in the reply record, and it can be delivered to the application.

## Advanced configuration for the Kafka consumer

The underlying Kafka consumer can be configured with the `reply` property prefix.
For example the underlying Kafka consuemer can be configured to batch mode using:

```properties
mp.messaging.outgoing.reqrep.topic=requests
mp.messaging.outgoing.reqrep.reply.topic=quote-results
mp.messaging.outgoing.reqrep.reply.batch=true
mp.messaging.outgoing.reqrep.reply.commit-strategy=latest
```
