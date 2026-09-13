# Receiving messages from RabbitMQ

The RabbitMQ OG connector lets you retrieve messages from a [RabbitMQ
broker](https://www.rabbitmq.com/). The RabbitMQ connector retrieves
*RabbitMQ Messages* and maps each of them into Reactive Messaging
`Messages`.

!!!note
    In this context, the reactive messaging concept of a *Channel* is
    realised as a [RabbitMQ Queue](https://www.rabbitmq.com/queues.html).

## Example

Let's imagine you have a RabbitMQ broker running, and accessible using
the `rabbitmq:5672` address (by default it would use `localhost:5672`).
Configure your application to receive RabbitMQ Messages on the `prices`
channel as follows:

``` properties
rabbitmq-host=rabbitmq  # <1>
rabbitmq-port=5672      # <2>
rabbitmq-username=my-username   # <3>
rabbitmq-password=my-password   # <4>

mp.messaging.incoming.prices.connector=smallrye-rabbitmq-og # <5>
mp.messaging.incoming.prices.queue.name=my-queue            # <6>
mp.messaging.incoming.prices.routing-keys=urgent            # <7>
```

1.  Configures the broker/router host name. You can do it per channel
    (using the `host` attribute) or globally using `rabbitmq-host`.

2.  Configures the broker/router port. You can do it per channel (using
    the `port` attribute) or globally using `rabbitmq-port`. The default
    is 5672.

3.  Configures the broker/router username if required. You can do it per
    channel (using the `username` attribute) or globally using
    `rabbitmq-username`.

4.  Configures the broker/router password if required. You can do it per
    channel (using the `password` attribute) or globally using
    `rabbitmq-password`.

5.  Instructs the `prices` channel to be managed by the RabbitMQ OG
    connector.

6.  Configures the RabbitMQ queue to read messages from.

7.  Configures the binding between the RabbitMQ exchange and the
    RabbitMQ queue using a routing key. The default is `#` (all messages
    will be forwarded from the exchange to the queue) but in general
    this can be a comma-separated list of one or more keys.

Then, your application receives `Message<String>`. You can consume the
payload directly:

``` java
{{ insert('rabbitmq/og/inbound/RabbitMQPriceConsumer.java') }}
```

Or, you can retrieve the `Message<String>`:

``` java
{{ insert('rabbitmq/og/inbound/RabbitMQPriceMessageConsumer.java') }}
```

!!!note
    Whether you need to explicitly acknowledge the message depends on the
    `auto-acknowledgement` channel setting; if that is set to `true` then
    your message will be automatically acknowledged on receipt.

## Deserialization

The connector converts incoming RabbitMQ Messages into Reactive
Messaging `Message<T>` instances. Incoming messages are received as
`byte[]` and can be automatically converted to `String` using the
built-in message converter.

| content_type             | Type     |
|--------------------------|----------|
| `text/plain`             | `String` |
| *Anything else or unset* | `byte[]` |

!!!note
    Unlike the Vert.x-based RabbitMQ connector, the OG connector does not
    provide automatic conversion to Vert.x `JsonObject` or `JsonArray`
    types. For JSON payloads, receive the message as `String` or `byte[]`
    and use your preferred JSON library (such as Jackson) for
    deserialization.

## Inbound Metadata

Messages coming from RabbitMQ contain an instance of {{ javadoc('io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-rabbitmq-og') }}
in the metadata.

RabbitMQ message headers can be accessed from the metadata either by
calling `getHeader(String header)` to retrieve a single
header value as a `String`, or `getHeader(String header, Class<T> type)` to retrieve
a typed header value, or `getHeaders()` to get a map of all header values.

``` java
{{ insert('rabbitmq/og/inbound/RabbitMQMetadataExample.java', 'code') }}
```

The type `<T>` of the header value depends on the RabbitMQ type used for
the header:

| RabbitMQ Header Type | T                |
|----------------------|------------------|
| String               | `String`         |
| Boolean              | `Boolean`        |
| Number               | `Number`         |
| List                 | `java.util.List` |

!!!note
    The `IncomingRabbitMQMetadata` in the OG connector returns direct types
    (e.g. `String`, `Integer`, `Date`) for most property accessors rather
    than `Optional` wrappers. Values will be `null` when not set by the
    producer. The `getHeader` methods still return `Optional`.

## Acknowledgement

When a Reactive Messaging Message associated with a RabbitMQ Message is
acknowledged, it informs the broker that the message has been
*accepted*.

Whether you need to explicitly acknowledge the message depends on the
`auto-acknowledgement` setting for the channel; if that is set to `true`
then your message will be automatically acknowledged on receipt.

## Failure Management

If a message produced from a RabbitMQ message is *nacked*, a failure
strategy is applied. The RabbitMQ OG connector supports four strategies,
controlled by the `failure-strategy` channel setting:

-   `fail` - fail the application; no more RabbitMQ messages will be
    processed. The RabbitMQ message is marked as rejected.

-   `accept` - this strategy marks the RabbitMQ message as accepted. The
    processing continues ignoring the failure.

-   `reject` - this strategy marks the RabbitMQ message as rejected
    (default). The processing continues with the next message.

-   `requeue` - this strategy marks the RabbitMQ message as rejected
    with requeue flag to true. The processing continues with the next message,
    but the requeued message will be redelivered to the consumer.

The RabbitMQ reject `requeue` flag can be controlled on different failure strategies
using the {{ javadoc('io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQRejectMetadata') }}.
To do that, use the `Message.nack(Throwable, Metadata)` method by including the
`RabbitMQRejectMetadata` metadata with `requeue` to `true`.

``` java
{{ insert('rabbitmq/og/inbound/RabbitMQRejectMetadataExample.java', 'code') }}
```

!!!warning "Experimental"
    `RabbitMQFailureHandler` is experimental and APIs are subject to change in the future

In addition, you can also provide your own failure strategy.
To provide a failure strategy implement a bean exposing the interface
{{ javadoc('io.smallrye.reactive.messaging.rabbitmq.og.fault.RabbitMQFailureHandler') }},
qualified with a `@Identifier`.
Set the name of the bean as the `failure-strategy` channel setting.


## Configuration Reference

{{ insert('../../../target/connectors/smallrye-rabbitmq-og-incoming.md') }}

To use an existing *queue*, you need to configure the `queue.name`
attribute.

For example, if you have a RabbitMQ broker already configured with a
queue called `peopleQueue` that you wish to read messages from, you need
the following configuration:

``` properties
mp.messaging.incoming.people.connector=smallrye-rabbitmq-og
mp.messaging.incoming.people.queue.name=peopleQueue
```

If you want RabbitMQ to create the queue for you but bind it to an
existing topic exchange `people`, you need the following configuration:

``` properties
mp.messaging.incoming.people.connector=smallrye-rabbitmq-og
mp.messaging.incoming.people.queue.name=peopleQueue
mp.messaging.incoming.people.queue.declare=true
```

!!!note
    In the above the channel name `people` is implicitly assumed to be the
    name of the exchange; if this is not the case you would need to name the
    exchange explicitly using the `exchange.name` property.

!!!note
    The connector supports RabbitMQ's "Server-named Queues" feature to create
    an exclusive, auto-deleting, non-durable and randomly named queue. To
    enable this feature you set the queue name to exactly `(server.auto)`.
    Using this name not only enables the server named queue feature but also
    automatically makes ths queue exclusive, auto-deleting, and non-durable;
    therefore ignoring any values provided for the `exclusive`, `auto-delete`
    and `durable` options.

If you want RabbitMQ to create the `people` exchange, queue and binding,
you need the following configuration:

``` properties
mp.messaging.incoming.people.connector=smallrye-rabbitmq-og
mp.messaging.incoming.people.exchange.declare=true
mp.messaging.incoming.people.queue.name=peopleQueue
mp.messaging.incoming.people.queue.declare=true
mp.messaging.incoming.people.queue.routing-keys=tall,short
```

In the above we have used an explicit list of routing keys rather than
the default (`#`). Each component of the list creates a separate binding
between the queue and the exchange, so in the case above we would have
two bindings; one based on a routing key of `tall`, the other based on
one of `short`.

!!!note
    The default value of `routing-keys` is `#` (indicating a match against
    all possible routing keys) which is only appropriate for *topic*
    Exchanges. If you are using other types of exchange and/or need to
    declare queue bindings, you'll need to supply a valid value for the
    exchange in question.

## Custom arguments for Queue declaration

When queue declaration is made by the Reactive Messaging channel, using the `queue.declare=true` configuration,
custom queue arguments can be specified using the `queue.arguments` attribute.
`queue.arguments` accepts the identifier (using the `@Identifier` qualifier) of a `Map<String, Object>` exposed as a CDI bean.
If no arguments has been configured, the default **rabbitmq-queue-arguments** identifier is looked for.

The following CDI bean produces such a configuration identified with **my-arguments**:

``` java
{{ insert('rabbitmq/og/customization/ArgumentProducers.java') }}
```

Then the channel can be configured to use those arguments in queue declaration:

```properties
mp.messaging.incoming.data.queue.arguments=my-arguments
```

Similarly, the `dead-letter-queue.arguments` allows configuring custom arguments for dead letter queue when one is declared (`auto-bind-dlq=true`).

## Consumer configuration

The OG connector provides additional consumer options:

-   `consumer-tag` - a custom consumer tag; if not provided, the broker
    generates one automatically.

-   `consumer-exclusive` - whether the consumer has exclusive access to
    the queue.

-   `consumer-arguments` - a comma-separated list of arguments
    (`key1:value1,key2:value2,...`) for the consumer.

-   `content-type-override` - overrides the `content_type` attribute of
    the incoming message; should be a valid MIME type.

-   `max-outstanding-messages` - the maximum number of unacknowledged
    messages being processed concurrently. This controls backpressure
    through RabbitMQ's QoS (prefetch count) mechanism.
