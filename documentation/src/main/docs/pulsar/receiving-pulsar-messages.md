# Receiving messages from Pulsar

The Pulsar Connector connects to a Pulsar broker using a Pulsar client and creates consumers to
receive messages from Pulsar brokers and it maps each of them into Reactive Messaging `Messages`.

## Example

Let’s imagine you have a Pulsar broker running, and accessible
using the `pulsar:6650` address (by default it would use
`localhost:6650`). Configure your application to receive Pulsar messages
on the `prices` channel as follows:

```properties
mp.messaging.incoming.prices.connector=smallrye-pulsar # <1>
mp.messaging.incoming.prices.serviceUrl=pulsar://pulsar:6650 # <2>
mp.messaging.incoming.prices.schema=DOUBLE # <3>
mp.messaging.incoming.prices.subscriptionInitialPosition=Earliest # <4>
```

1.  Sets the connector for the `prices` channel
2.  Configure the Pulsar broker service url.
3.  Configure the schema to consume prices as Double.
4.  Make sure consumer subscription starts receiving messages from the `Earliest` position.


!!!note
    You don’t need to set the Pulsar topic, nor the consumer name.
    By default, the connector uses the channel name (`prices`).
    You can configure the `topic` and `consumerName` attributes to override them.

!!!note
    In Pulsar, consumers need to provide a `subscriptionName` for topic subscriptions.
    If not provided the connector is generating a unique **subscription name**.

Then, your application can receive the `double` payload directly:

``` java
{{ insert('pulsar/inbound/PulsarPriceConsumer.java') }}
```

Or, you can retrieve the `Message<Double>`:

``` java
{{ insert('pulsar/inbound/PulsarPriceMessageConsumer.java') }}
```

## Consumer Configuration

The Pulsar Connector allows flexibly configuring the underlying Pulsar consumer.
One of the ways is to set consumer properties directly on the channel configuration.
The list of available configuration properties are listed in [Configuration Reference](#configuration-reference).

See the [Configuring Pulsar consumers, producers and clients](client-configuration.md) for more information.

## Deserialization and Pulsar Schema

The Pulsar Connector allows configuring Schema configuration for the underlying Pulsar consumer.
See the [Configuring the schema used for Pulsar channels](schema-configuration.md) for more information.

## Inbound Metadata

The incoming Pulsar messages include an instance of `PulsarIncomingMessageMetadata` in the metadata.
It provides the key, topic, partitions, headers and so on:

```java
{{ insert('pulsar/inbound/PulsarMetadataExample.java', 'code') }}
```

## Acknowledgement

When a message produced from a Pulsar Message is *acknowledged*, the connector sends an [acknowledgement request](https://pulsar.apache.org/docs/3.0.x/concepts-messaging/#acknowledgment) to the Pulsar broker.
All Reactive Messaging messages need to be *acknowledged*, which is handled automatically in most cases.
Acknowledgement requests can be sent to the Pulsar broker using the following two strategies:

-   **Individual acknowledgement** is the default strategy, an acknowledgement request is to the broker for each message.
-   **Cumulative acknowledgement**, configured using `ack-strategy=cumulative`, the consumer only acknowledges the last message it received.
All messages in the stream up to (and including) the provided message are not redelivered to that consumer.

## Failure Management

If a message produced from a Pulsar message is *nacked*, a failure strategy is applied.
The Pulsar connector supports 4 strategies:

-   `nack` *(default)* sends [negative acknowledgment](https://pulsar.apache.org/docs/3.0.x/concepts-messaging/#negative-acknowledgment) to the broker, triggering the broker to redeliver this message to the consumer.
The negative acknowledgment can be further configured using `negativeAckRedeliveryDelayMicros` and `negativeAck.redeliveryBackoff` properties.
-   `fail` fail the application, no more messages will be processed.
-   `ignore` the failure is logged, but the acknowledgement strategy will be applied and the processing will continue.
-   `continue` the failure is logged, but processing continues without applying acknowledgement or negative acknowledgement. This strategy can be used with [acknowledgement timeout](#acknowledgement-timeout) configuration.
-   `reconsume-later` sends the message to the [retry letter topic](https://pulsar.apache.org/docs/3.0.x/concepts-messaging/#retry-letter-topic) using the `reconsumeLater` API to be reconsumed with a delay.
The delay can be configured using the `reconsumeLater.delay` property and defaults to 3 seconds.
Custom delay or properties per message can be configured by adding an instance of {{ javadoc('io.smallrye.reactive.messaging.pulsar.PulsarReconsumeLaterMetadata') }} to the failure metadata.

For example the following configuration for the incoming channel `data` uses `reconsumer-later` failure strategy with default delays of 60 seconds:

```properties
mp.messaging.incoming.data.connector=smallrye-pulsar
mp.messaging.incoming.data.serviceUrl=pulsar://localhost:6650
mp.messaging.incoming.data.topic=data
mp.messaging.incoming.data.schema=INT32
mp.messaging.incoming.data.failure-strategy=reconsume-later
mp.messaging.incoming.data.retryEnable=true
mp.messaging.incoming.data.reconsumeLater.delay=60 // in seconds
mp.messaging.incoming.data.deadLetterPolicy.retryLetterTopic=data-retry
mp.messaging.incoming.data.deadLetterPolicy.maxRedeliverCount=2
```

### Acknowledgement timeout

Similar to the negative acknowledgement, with the [acknowledgment timeout](https://pulsar.apache.org/docs/3.0.x/concepts-messaging/#acknowledgment-timeout) mechanism, the Pulsar client tracks the unacknowledged messages,
for the given *ackTimeout* period and sends *redeliver unacknowledged messages request* to the broker, thus the broker resends the unacknowledged messages to the consumer.

To configure the timeout and redelivery backoff mechanism you can set `ackTimeoutMillis` and `ackTimeout.redeliveryBackoff` properties.
The `ackTimeout.redeliveryBackoff` value accepts comma separated values of min delay in milliseconds, max delay in milliseconds and multiplier respectively:

```properties
mp.messaging.incoming.data.connector=smallrye-pulsar
mp.messaging.incoming.data.failure-strategy=continue
mp.messaging.incoming.data.ackTimeoutMillis=10000
mp.messaging.incoming.data.ackTimeout.redeliveryBackoff=1000,60000,2
```

### Dead-letter topic

The [dead letter topic](https://pulsar.apache.org/docs/3.0.x/concepts-messaging/#dead-letter-topic) pushes messages that are not consumed successfully to a dead letter topic an continue message consumption.
Note that dead letter topic can be used in different message redelivery methods, such as acknowledgment timeout, negative acknowledgment or retry letter topic.

```properties
mp.messaging.incoming.data.connector=smallrye-pulsar
mp.messaging.incoming.data.failure-strategy=nack
mp.messaging.incoming.data.deadLetterPolicy.maxRedeliverCount=2
mp.messaging.incoming.data.deadLetterPolicy.deadLetterTopic=my-dead-letter-topic
mp.messaging.incoming.data.deadLetterPolicy.initialSubscriptionName=my-dlq-subscription
mp.messaging.incoming.data.subscriptionType=Shared
```

!!!Important
    *Negative acknowledgment* or *acknowledgment timeout* methods for redelivery will redeliver the whole batch of messages containing at least an unprocessed message.
    See [producer batching](sending-messages-to-pulsar.md#producer-batching) for more information.

## Receiving Pulsar Messages in Batches

By default, incoming methods receive each Pulsar message individually.
You can enable batch mode using `batchReceive=true` property, or setting a `batchReceivePolicy` in consumer configuration.

```java
{{ insert('pulsar/inbound/PulsarMessageBatchExample.java', 'code') }}
```

Or you can directly receive the list of payloads to the consume method:

```java
{{ insert('pulsar/inbound/PulsarMessageBatchPayloadExample.java', 'code') }}
```

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-pulsar-incoming.md') }}

In addition to the configuration properties provided by the connector,
following Pulsar consumer properties can also be set on the channel:

{{ insert('../docs/pulsar/config/smallrye-pulsar-consumer.md') }}

