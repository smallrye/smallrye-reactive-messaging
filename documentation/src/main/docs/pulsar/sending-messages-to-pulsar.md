# Sending messages to Pulsar

The Pulsar Connector can write Reactive Messaging `Message`s as Pulsar Message.

## Example

Let’s imagine you have a Pulsar broker running, and accessible
using the `pulsar:6650` address (by default it would use
`localhost:1883`). Configure your application to write the messages from
the `prices` channel into a Pulsar Messages as follows:

```properties
mp.messaging.outgoing.prices.connector=smallrye-pulsar # <1>
mp.messaging.outgoing.prices.serviceUrl=pulsar://pulsar:6650 # <2>
mp.messaging.outgoing.prices.schema=DOUBLE # <3>
```

1.  Sets the connector for the `prices` channel
2.  Configure the Pulsar broker service url.
3.  Configure the schema to consume prices as Double.

!!!note
    You don’t need to set the Pulsar topic, nor the producer name.
    By default, the connector uses the channel name (`prices`).
    You can configure the `topic` and `producerName` attributes to override them.

Then, your application must send `Message<Double>` to the `prices`
channel. It can use `double` payloads as in the following snippet:

``` java
{{ insert('pulsar/outbound/PulsarPriceProducer.java') }}
```

Or, you can send `Message<Double>`:

``` java
{{ insert('pulsar/outbound/PulsarPriceMessageProducer.java') }}
```

## Producer Configuration

The Pulsar Connector allows flexibly configuring the underlying Pulsar producer.
One of the ways is to set producer properties directly on the channel configuration.
The list of available configuration properties are listed in [Configuration Reference](#configuration-reference).

See the [Configuring Pulsar consumers, producers and clients](client-configuration.md) for more information.

## Serialization and Pulsar Schema

The Pulsar Connector allows configuring Schema configuration for the underlying Pulsar producer.
See the [Configuring the schema used for Pulsar channels](schema-configuration.md) for more information.

## Sending key/value pairs

In order to send Kev/Value pairs to Pulsar, you can configure the Pulsar producer Schema with a
{{ javadoc('org.apache.pulsar.common.schema.KeyValue', True, 'org.apache.pulsar/pulsar-client-api') }} type:

``` java
{{ insert('pulsar/outbound/PulsarKeyValueExample.java', 'code') }}
```

If you need more control on the written records, use
`PulsarOutgoingMessageMetadata`.

## Outbound Metadata

When sending `Message`s, you can add an instance of
{{ javadoc('io.smallrye.reactive.messaging.pulsar.PulsarOutgoingMessageMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-pulsar') }}
to influence how the message is going to be written to Pulsar.
For example, configure the record key, and set message properties:

``` java
{{ insert('pulsar/outbound/PulsarOutboundMetadataExample.java', 'code') }}
```

## OutgoingMessage

Using {{ javadoc('io.smallrye.reactive.messaging.pulsar.OutgoingMessage', False, 'io.smallrye.reactive/smallrye-reactive-messaging-pulsar') }},
is an easy way of customizing the Pulsar message to be published when dealing with payloads and not `Message`s.

You can create an `OutgoingMessage` with key and value, or from an incoming Pulsar Message:

``` java
{{ insert('pulsar/outbound/PulsarOutgoingMessageExample.java', 'code') }}
```

## Acknowledgement

Upon receiving a message from a Producer, a Pulsar broker assigns a `MessageId` to the message and sends it back to the producer,
confirming that the message is published.

By default, the connector does wait for Pulsar to acknowledge the record
to continue the processing (acknowledging the received `Message`).
You can disable this by setting the `waitForWriteCompletion` attribute to `false`.

If a record cannot be written, the message is `nacked`.

!!!Important
    The Pulsar client automatically retries sending messages in case of failure, until the *send timeout* is reached.
    The *send timeout* is configurable with `sendTimeoutMs` attribute and by default is is 30 seconds.

## Back-pressure and inflight records

The Pulsar outbound connector handles back-pressure monitoring the number
of pending messages waiting to be written to the Pulsar broker.
The number of pending messages is configured using the
`maxPendingMessages` attribute and defaults to 1000.

The connector only sends that amount of messages concurrently. No other
messages will be sent until at least one pending message gets
acknowledged by the broker. Then, the connector writes a new message to
Pulsar when one of the broker’s pending messages get acknowledged.

You can also remove the limit of pending messages by setting `maxPendingMessages` to `0`.
Note that Pulsar also enables to configure the number of pending messages per partition using `maxPendingMessagesAcrossPartitions`.

## Producer Batching

By default, the Pulsar producer batches individual messages together to be published to the broker.
You can configure batching parameters using `batchingMaxPublishDelayMicros`, `batchingPartitionSwitchFrequencyByPublishDelay`,
`batchingMaxMessages`, `batchingMaxBytes` configuration properties, or disable it completely with `batchingEnabled=false`.

When using `Key_Shared` consumer subscriptions, the `batcherBuilder` can be configured to `BatcherBuilder.KEY_BASED`.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-pulsar-outgoing.md') }}

In addition to the configuration properties provided by the connector,
following Pulsar producer properties can also be set on the channel:

{{ insert('../docs/pulsar/config/smallrye-pulsar-producer.md') }}

