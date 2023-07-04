# Receiving messages from RabbitMQ

The RabbitMQ connector lets you retrieve messages from a [RabbitMQ
broker](https://www.rabbitmq.com/). The RabbitMQ connector retrieves
*RabbitMQ Messages* and maps each of them into Reactive Messaging
`Messages`.

!!!note
    In this context, the reactive messaging concept of a *Channel* is
    realised as a [RabbitMQ Queue](https://www.rabbitmq.com/queues.html).

## Example

Let’s imagine you have a RabbitMQ broker running, and accessible using
the `rabbitmq:5672` address (by default it would use `localhost:5672`).
Configure your application to receive RabbitMQ Messages on the `prices`
channel as follows:

``` properties
rabbitmq-host=rabbitmq  # <1>
rabbitmq-port=5672      # <2>
rabbitmq-username=my-username   # <3>
rabbitmq-password=my-password   # <4>

mp.messaging.incoming.prices.connector=smallrye-rabbitmq # <5>
mp.messaging.incoming.prices.queue.name=my-queue         # <6>
mp.messaging.incoming.prices.routing-keys=urgent         # <7>
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

5.  Instructs the `prices` channel to be managed by the RabbitMQ
    connector.

6.  Configures the RabbitMQ queue to read messages from.

7.  Configures the binding between the RabbitMQ exchange and the
    RabbitMQ queue using a routing key. The default is `#` (all messages
    will be forwarded from the exchange to the queue) but in general
    this can be a comma-separated list of one or more keys.

Then, your application receives `Message<String>`. You can consume the
payload directly:

``` java
{{ insert('rabbitmq/inbound/RabbitMQPriceConsumer.java') }}
```

Or, you can retrieve the `Message<String>`:

``` java
{{ insert('rabbitmq/inbound/RabbitMQPriceMessageConsumer.java') }}
```

!!!note
    Whether you need to explicitly acknowledge the message depends on the
    `auto-acknowledgement` channel setting; if that is set to `true` then
    your message will be automatically acknowledged on receipt.

## Deserialization

The connector converts incoming RabbitMQ Messages into Reactive
Messaging `Message<T>` instances. The payload type `T` depends on the
value of the RabbitMQ received message Envelope `content_type` and
`content_encoding` properties.

| content_encoding | content_type       | Type                                                                                                                                                                                                                                                              |
|------------------|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| *Value present*  | *n/a*              | `byte[]`                                                                                                                                                                                                                                                          |
| *No value*       | `text/plain`       | `String`                                                                                                                                                                                                                                                          |
| *No value*       | `application/json` | a JSON element which can be a [`JsonArray`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonArray.html), [`JsonObject`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonObject.html), `String`, ... if the buffer contains an array, object, string,... |
| *No value*       | *Anything else*    | `byte[]`                                                                                                                                                                                                                                                          |

If you send objects with this RabbitMQ connector (outbound connector),
they are encoded as JSON and sent with `content_type` set to
`application/json`. You can receive this payload using (Vert.x) JSON
Objects, and then map it to the object class you want:

``` java
@ApplicationScoped
public static class Generator {

    @Outgoing("to-rabbitmq")
    public Multi<Price> prices() {             // <1>
        AtomicInteger count = new AtomicInteger();
        return Multi.createFrom().ticks().every(Duration.ofMillis(1000))
                .map(l -> new Price().setPrice(count.incrementAndGet()))
                .onOverflow().drop();
    }

}

@ApplicationScoped
public static class Consumer {

    List<Price> prices = new CopyOnWriteArrayList<>();

    @Incoming("from-rabbitmq")
    public void consume(JsonObject p) {      // <2>
        Price price = p.mapTo(Price.class);  // <3>
        prices.add(price);
    }

    public List<Price> list() {
        return prices;
    }
}
```

1.  The `Price` instances are automatically encoded to JSON by the
    connector
2.  You can receive it using a `JsonObject`
3.  Then, you can reconstruct the instance using the `mapTo` method

## Inbound Metadata

Messages coming from RabbitMQ contain an instance of {{ javadoc('io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-rabbitmq') }}
in the metadata.

RabbitMQ message headers can be accessed from the metadata either by
calling `getHeader(String header, Class<T> type)` to retrieve a single
header value. or `getHeaders()` to get a map of all header values.

``` java
{{ insert('rabbitmq/inbound/RabbitMQMetadataExample.java', 'code') }}
```

The type `<T>` of the header value depends on the RabbitMQ type used for
the header:

| RabbitMQ Header Type | T                |
|----------------------|------------------|
| String               | `String`         |
| Boolean              | `Boolean`        |
| Number               | `Number`         |
| List                 | `java.util.List` |

## Acknowledgement

When a Reactive Messaging Message associated with a RabbitMQ Message is
acknowledged, it informs the broker that the message has been
*accepted*.

Whether you need to explicitly acknowledge the message depends on the
`auto-acknowledgement` setting for the channel; if that is set to `true`
then your message will be automatically acknowledged on receipt.

## Failure Management

If a message produced from a RabbitMQ message is *nacked*, a failure
strategy is applied. The RabbitMQ connector supports three strategies,
controlled by the `failure-strategy` channel setting:

-   `fail` - fail the application; no more RabbitMQ messages will be
    processed. The RabbitMQ message is marked as rejected.

-   `accept` - this strategy marks the RabbitMQ message as accepted. The
    processing continues ignoring the failure.

-   `reject` - this strategy marks the RabbitMQ message as rejected
    (default). The processing continues with the next message.

!!!warning "Experimental"
    `RabbitMQFailureHandler` is experimental and APIs are subject to change in the future

In addition, you can also provide your own failure strategy. To provide a failure strategy implement a bean exposing the interface
{{ javadoc('io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler') }}, qualified with a `@Identifier`. Set the name of the bean
as the `failure-strategy` channel setting.


## Configuration Reference

{{ insert('../../../target/connectors/smallrye-rabbitmq-incoming.md') }}

To use an existing *queue*, you need to configure the `queue.name`
attribute.

For example, if you have a RabbitMQ broker already configured with a
queue called `peopleQueue` that you wish to read messages from, you need
the following configuration:

``` properties
mp.messaging.incoming.people.connector=smallrye-rabbitmq
mp.messaging.incoming.people.queue.name=peopleQueue
```

If you want RabbitMQ to create the queue for you but bind it to an
existing topic exchange `people`, you need the following configuration:

``` properties
mp.messaging.incoming.people.connector=smallrye-rabbitmq
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
mp.messaging.incoming.people.connector=smallrye-rabbitmq
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
    declare queue bindings, you’ll need to supply a valid value for the
    exchange in question.

