# Sending messages to RabbitMQ

The RabbitMQ connector can write Reactive Messaging `Messages` as
RabbitMQ Messages.

!!!note
    In this context, the reactive messaging concept of a *Channel* is
    realised as a [RabbitMQ
    Exchange](https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges).

## Example

Let’s imagine you have a RabbitMQ broker running, and accessible using
the `rabbitmq:5672` address (by default it would use `localhost:5672`).
Configure your application to send the messages from the `prices`
channel as a RabbitMQ Message as follows:

```
rabbitmq-host=rabbitmq   # <1>
rabbitmq-port=5672       # <2>
rabbitmq-username=my-username # <3>
rabbitmq-password=my-password  # <4>

mp.messaging.outgoing.prices.connector=smallrye-rabbitmq # <5>
mp.messaging.outgoing.prices.default-routing-key=normal    # <6>
```

1.  Configures the broker/router host name. You can do it per channel
    (using the `host` attribute) or globally using `rabbitmq-host`

2.  Configures the broker/router port. You can do it per channel (using
    the `port` attribute) or globally using `rabbitmq-port`. The default
    is `5672`.

3.  Configures the broker/router username if required. You can do it per
    channel (using the `username` attribute) or globally using
    `rabbitmq-username`.

4.  Configures the broker/router password if required. You can do it per
    channel (using the `password` attribute) or globally using
    `rabbitmq-password`.

5.  Instructs the `prices` channel to be managed by the RabbitMQ
    connector

6.  Supplies the default routing key to be included in outbound
    messages; this will be if the "raw payload" form of message sending
    is used (see below).

!!!note
    You don’t need to set the RabbitMQ exchange name. By default, it uses
    the channel name (`prices`) as the name of the exchange to send messages
    to. You can configure the `exchange.name` attribute to override it.

Then, your application can send `Message<Double>` to the prices channel.
It can use `double` payloads as in the following snippet:

``` java
{{ insert('rabbitmq/outbound/RabbitMQPriceProducer.java') }}
```

Or, you can send `Message<Double>`, which affords the opportunity to
explicitly specify metadata on the outgoing message:

``` java
{{ insert('rabbitmq/outbound/RabbitMQPriceMessageProducer.java') }}
```

## Serialization

When sending a `Message<T>`, the connector converts the message into a
RabbitMQ Message. The payload is converted to the RabbitMQ Message body.

| T                                                                                                                                                                  | RabbitMQ Message Body                                                                                                |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| primitive types or `UUID`/`String`                                                                                                                                 | String value with `content_type` set to `text/plain`                                                                 |
| [`JsonObject`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonObject.html) or [`JsonArray`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonArray.html) | Serialized String payload with `content_type` set to `application/json`                                              |
| `io.vertx.mutiny.core.buffer.Buffer`                                                                                                                               | Binary content, with `content_type` set to `application/octet-stream`                                                |
| `byte[]`                                                                                                                                                           | Binary content, with content_type set to `application/octet-stream`                                                  |
| Any other class                                                                                                                                                    | The payload is converted to JSON (using a Json Mapper) then serialized with `content_type` set to `application/json` |

If the message payload cannot be serialized to JSON, the message is
*nacked*.

## Outbound Metadata

When sending `Messages`, you can add an instance of {{ javadoc('io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-rabbitmq') }}
to influence how the message is handled by RabbitMQ. For example, you
can configure the routing key, timestamp and headers:

``` java
{{ insert('rabbitmq/outbound/RabbitMQOutboundMetadataExample.java', 'code') }}
```

## Acknowledgement

By default, the Reactive Messaging `Message` is acknowledged when the
broker acknowledges the message.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-rabbitmq-outgoing.md') }}

## Using existing destinations

To use an existing *exchange*, you may need to configure the
`exchange.name` attribute.

For example, if you have a RabbitMQ broker already configured with an
exchange called `people` that you wish to send messages to, you need the
following configuration:

``` properties
mp.messaging.outgoing.people.connector=smallrye-rabbitmq
```

You would need to configure the `exchange.name` attribute, if the
exchange name were not the channel name:

``` properties
mp.messaging.outgoing.people-out.connector=smallrye-rabbitmq
mp.messaging.outgoing.people-out.exchange.name=people
```

If you want RabbitMQ to create the `people` exchange, you need the
following configuration:

``` properties
mp.messaging.outgoing.people-out.connector=smallrye-amqp
mp.messaging.outgoing.people-out.exchange.name=people
mp.messaging.outgoing.people-out.exchange.declare=true
```

!!!note
    The above example will create a `topic` exchange and use an empty
    default `routing-key` (unless overridden programatically using outgoing
    metadata for the message). If you want to create a different type of
    exchange or have a different default routing key, then the
    `exchange.type` and `default-routing-key` properties need to be
    explicitly specified.
