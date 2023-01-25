# Writing Kafka Records

The Kafka Connector can write Reactive Messaging `Messages` as Kafka
Records.

## Example

Let’s imagine you have a Kafka broker running, and accessible using the
`kafka:9092` address (by default it would use `localhost:9092`).
Configure your application to write the messages from the `prices`
channel into a Kafka *topic* as follows:

```properties
kafka.bootstrap.servers=kafka:9092 # <1>

mp.messaging.outgoing.prices-out.connector=smallrye-kafka # <2>
mp.messaging.outgoing.prices-out.value.serializer=org.apache.kafka.common.serialization.DoubleSerializer # <3>
mp.messaging.outgoing.prices-out.topic=prices # <4>
```
1.  Configure the broker location. You can configure it globally or per
    channel
2.  Configure the connector to manage the `prices` channel
3.  Sets the (Kafka) serializer to encode the message payload into the
    record’s value
4.  Make sure the topic name is `prices` (and not the default
    `prices-out`)

Then, your application must send `Message<Double>` to the `prices`
channel. It can use `double` payloads as in the following snippet:

``` java
{{ insert('kafka/outbound/KafkaPriceProducer.java') }}
```

Or, you can send `Message<Double>`:

``` java
{{ insert('kafka/outbound/KafkaPriceMessageProducer.java') }}
```

## Serialization

The serialization is handled by the underlying Kafka Client. You need to
configure the:

-   `mp.messaging.outgoing.[channel-name].value.serializer` to configure
    the value serializer (mandatory)

-   `mp.messaging.outgoing.[channel-name].key.serializer` to configure
    the key serializer (optional, default to `String`)

If you want to use a custom serializer, add it to your `CLASSPATH` and
configure the associate attribute.

By default, the written record contains:

-   the `Message` payload as *value*

-   no key, or the key configured using the `key` attribute or the key
    passed in the metadata attached to the `Message`

-   the timestamp computed for the system clock (`now`) or the timestamp
    passed in the metadata attached to the `Message`

## Sending key/value pairs

In the Kafka world, it’s often necessary to send *records*, i.e. a
key/value pair. The connector provides the {{ javadoc('io.smallrye.reactive.messaging.kafka.Record', True, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }}
class that you can use to send a pair:

``` java
{{ insert('kafka/outbound/KafkaRecordExample.java', 'code') }}
```

When the connector receives a message with a `Record` payload, it
extracts the key and value from it. The configured serializers for the
key and the value must be compatible with the record’s key and value.
Note that the `key` and the `value` can be `null`. It is also possible
to create a record with a `null` key AND a `null` value.

If you need more control on the written records, use
`OutgoingKafkaRecordMetadata`.

## Outbound Metadata

When sending `Messages`, you can add an instance of
{{ javadoc('io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }}
to influence how the message is going to be written to Kafka. For example,
you can add Kafka headers, configure the record key…

``` java
{{ insert('kafka/outbound/KafkaOutboundMetadataExample.java', 'code') }}
```

## Propagating Record Key

When processing messages, you can propagate incoming record key to the
outgoing record.

Consider the following example method, which consumes messages from the
channel `in`, transforms the payload, and writes the result to the
channel `out`.

``` java
{{ insert('kafka/outbound/KafkaPriceProcessor.java', 'code') }}
```

Enabled with
`mp.messaging.outgoing.[channel-name].propagate-record-key=true`
configuration, record key propagation produces the outgoing record with
the same *key* as the incoming record.

If the outgoing record already contains a *key*, it **won’t be
overridden** by the incoming record key. If the incoming record does
have a *null* key, the `mp.messaging.outgoing.[channel-name].key`
property is used.

## Propagating Record headers

You can also propagate incoming record headers to the outgoing record, by specifying the list of headers to be considered.

 `mp.messaging.outgoing.[channel-name].propagate-headers=Authorization,Proxy-Authorization`

If the ougoing record already defines a header with the same key, it won't be overriden by the incoming header.

## Dynamic topic names

Sometimes it is desirable to select the destination of a message
dynamically. In this case, you should not configure the topic inside
your application configuration file, but instead, use the outbound
metadata to set the name of the topic.

For example, you can route to a dynamic topic based on the incoming
message:

``` java
{{ insert('kafka/outbound/KafkaOutboundDynamicTopicExample.java', 'code') }}
```

## Acknowledgement

Kafka acknowledgement can take times depending on the configuration.
Also, it stores in-memory the records that cannot be written.

By default, the connector does wait for Kafka to acknowledge the record
to continue the processing (acknowledging the received `Message`). You
can disable this by setting the `waitForWriteCompletion` attribute to
`false`.

Note that the `acks` attribute has a huge impact on the record
acknowledgement.

If a record cannot be written, the message is `nacked`.

## Back-pressure and inflight records

The Kafka outbound connector handles back-pressure monitoring the number
of in-flight messages waiting to be written to the Kafka broker. The
number of in-flight messages is configured using the
`max-inflight-messages` attribute and defaults to 1024.

The connector only sends that amount of messages concurrently. No other
messages will be sent until at least one in-flight message gets
acknowledged by the broker. Then, the connector writes a new message to
Kafka when one of the broker’s in-flight messages get acknowledged. Be
sure to configure Kafka’s `batch.size` and `linger.ms` accordingly.

You can also remove the limit of inflight messages by setting
`max-inflight-messages` to `0`. However, note that the Kafka Producer
may block if the number of requests reaches
`max.in.flight.requests.per.connection`.

## Handling serialization failures

For Kafka producer client serialization failures are not recoverable,
thus the message dispatch is not retried. However, using schema
registries, serialization may intermittently fail to contact the
registry. In these cases you may need to apply a failure strategy for
the serializer. To achieve this, create a CDI bean implementing the {{ javadoc('io.smallrye.reactive.messaging.kafka.SerializationFailureHandler', False, 'io.smallrye.reactive/smallrye-reactive-messaging-kafka') }}
interface:

``` java
@ApplicationScoped
@Identifier("failure-fallback") // Set the name of the failure handler
public class MySerializationFailureHandler
    implements SerializationFailureHandler<JsonObject> { // Specify the expected type

    @Override
    public byte[] decorateSerialization(Uni<byte[]> serialization, String topic,
                boolean isKey, String serializer,
                Object data, Headers headers) {
        return serialization
                    .onFailure().retry().atMost(3)
                    .await().atMost(Duration.ofMillis(200));
    }
}
```

The bean must be exposed with the `@Identifier` qualifier specifying the
name of the bean. Then, in the connector configuration, specify the
following attribute:

-   `mp.messaging.incoming.$channel.key-serialization-failure-handler`:
    name of the bean handling serialization failures happening for the
    record’s key

-   `mp.messaging.incoming.$channel.value-serialization-failure-handler`:
    name of the bean handling serialization failures happening for the
    record’s value,

The handler is called with the serialization action as a `Uni`, the
record’s topic, a boolean indicating whether the failure happened on a
key, the class name of the deserializer that throws the exception, the
corrupted data, the exception, and the records headers. Failure
strategies like retry, providing a fallback value or applying timeout
can be implemented. Note that the method must await on the result and
return the serialized byte array. Alternatively, the handler can
implement `decorateSerialization` method which can return a fallback
value.

## Sending Cloud Events

The Kafka connector supports [Cloud Events](https://cloudevents.io/).
The connector sends the outbound record as Cloud Events if:

-   the message metadata contains an
    `io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata`
    instance,

-   the channel configuration defines the `cloud-events-type` and
    `cloud-events-source` attributes.

You can create
`io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata` instances
using:

``` java
{{ insert('kafka/outbound/KafkaCloudEventProcessor.java') }}
```

If the metadata does not contain an id, the connector generates one
(random UUID). The `type` and `source` can be configured per message or
at the channel level using the `cloud-events-type` and
`cloud-events-source` attributes. Other attributes are also
configurable.

The metadata can be contributed by multiple methods, however, you must
always retrieve the already existing metadata to avoid overriding the
values:

``` java
{{ insert('kafka/outbound/KafkaCloudEventMultipleProcessors.java') }}
```

By default, the connector sends the Cloud Events using the *binary*
format. You can write *structured* Cloud Events by setting the
`cloud-events-mode` to `structured`. Only JSON is supported, so the
created records had its `content-type` header set to
`application/cloudevents+json; charset=UTF-8` When using the
*structured* mode, the value serializer must be set to
`org.apache.kafka.common.serialization.StringSerializer`, otherwise the
connector reports the error. In addition, in *structured*, the connector
maps the message’s payload to JSON, except for `String` passed directly.

The record’s key can be set in the channel configuration (`key`
attribute), in the `OutgoingKafkaRecordMetadata` or using the
`partitionkey` Cloud Event attribute.

!!!note
    you can disable the Cloud Event support by setting the `cloud-events`
    attribute to `false`

## Using `ProducerRecord`

Kafka built-in type
[ProducerRecord\<K,V>](https://kafka.apache.org/26/javadoc/index.html?org/apache/kafka/clients/producer/ProducerRecord.html)
can also be used for producing messages:

``` java
{{ insert('kafka/outbound/KafkaProducerRecordExample.java', 'code') }}
```

!!!warning
    This is an advanced feature. The `ProducerRecord` is sent to Kafka as
    is. Any possible metadata attached through `Message<ProducerRecord<K, V>>` are ignored and lost.

## Producer Interceptors

Producer interceptors can be configured for Kafka producer clients using the standard `interceptor.classes` property.
Configured classes will be instantiated by the Kafka producer on client creation.

Alternatively, producer clients can be configured with a CDI managed-bean implementing {{ javadoc('org.apache.kafka.clients.producer.ProducerInterceptor', True, 'org.apache.kafka/kafka-clients') }} interface:

To achieve this, the bean must be exposed with the `@Identifier` qualifier specifying the name of the bean:


``` java
{{ insert('kafka/outbound/ProducerInterceptorBeanExample.java') }}
```

Then, in the channel configuration, specify the following attribute:
`mp.messaging.incoming.$channel.interceptor-bean=my-producer-interceptor`.

!!!warning
    The `onSend` method will be called on the producer *sending thread* and `onAcknowledgement` will be called on the *Kafka producer I/O thread*.
    In both cases if implementations are not fast, sending of messages could be delayed.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-kafka-outgoing.md') }}

You can also pass any property supported by the underlying [Kafka
producer](https://kafka.apache.org/documentation/#producerconfigs).

For example, to configure the `batch.size` property, use:

``` properties
mp.messaging.outgoing.[channel].batch.size=32768
```

Some producer client properties are configured to sensible default
values:

If not set, `reconnect.backoff.max.ms` is set to `10000` to avoid high
load on disconnection.

If not set, `key.serializer` is set to
`org.apache.kafka.common.serialization.StringSerializer`.

If not set, producer `client.id` is generated as
`kafka-producer-[channel]`.
