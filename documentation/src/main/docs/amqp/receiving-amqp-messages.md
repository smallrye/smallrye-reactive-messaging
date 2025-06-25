# Receiving messages from AMQP

The AMQP connector lets you retrieve messages from an [AMQP broker or
router](https://www.amqp.org/product/architecture). The AMQP connector
retrieves *AMQP Messages* and maps each of them into Reactive Messaging
`Messages`.

## Example

Let’s imagine you have an AMQP broker (such as [Apache ActiveMQ
Artemis](https://activemq.apache.org/components/artemis/)) running, and
accessible using the `amqp:5672` address (by default it would use
`localhost:5672`). Configure your application to receive AMQP Messages
on the `prices` channel as follows:

```properties
amqp-host=amqp # <1>
amqp-port=5672 # <2>
amqp-username=my-username # <3>
amqp-password=my-password # <4>

mp.messaging.incoming.prices.connector=smallrye-amqp # <5>
```

1.  Configures the broker/router host name. You can do it per channel
    (using the `host` attribute) or globally using `amqp-host`

2.  Configures the broker/router port. You can do it per channel (using
    the `port` attribute) or globally using `amqp-port`. The default is
    `5672`.

3.  Configures the broker/router username if required. You can do it per
    channel (using the `username` attribute) or globally using
    `amqp-username`.

4.  Configures the broker/router password if required. You can do it per
    channel (using the `password` attribute) or globally using
    `amqp-password`.

5.  Instructs the `prices` channel to be managed by the AMQP connector

!!!note
    You don’t need to set the AMQP *address*. By default, it uses the
    channel name (`prices`). You can configure the `address` attribute to
    override it.

Then, your application receives `Message<Double>`. You can consume the
payload directly:

``` java
{{ insert('amqp/inbound/AmqpPriceConsumer.java') }}
```

Or, you can retrieve the `Message<Double>`:

``` java
{{ insert('amqp/inbound/AmqpPriceMessageConsumer.java') }}
```

## Deserialization

The connector converts incoming AMQP Messages into Reactive Messaging
`Message<T>` instances. `T` depends on the *body* of the received AMQP
Message.

The [AMQP Type
System](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html)
defines the supported types.

| AMQP Body Type                                                                                                                                              | `<T>`                                                                            |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| AMQP Value containing a [AMQP Primitive Type](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#section-primitive-type-definitions) | the corresponding Java type                                                      |
| AMQP Value using the `Binary` type                                                                                                                          | `byte[]`                                                                         |
| AMQP Sequence                                                                                                                                               | `List`                                                                           |
| AMQP Data (with binary content) and the `content-type` is set to `application/json`                                                                         | [`JsonObject`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonObject.html) |
| AMQP Data with a different `content-type`                                                                                                                   | `byte[]`                                                                         |

If you send objects with this AMQP connector (outbound connector), it
gets encoded as JSON and sent as binary. The `content-type` is set to
`application/json`. You can receive this payload using (Vert.x) JSON
Objects, and then map it to the object class you want:

``` java
@ApplicationScoped
public static class Generator {

    @Outgoing("to-amqp")
    public Multi<Price> prices() {                      // <1>
        AtomicInteger count = new AtomicInteger();
        return Multi.createFrom().ticks().every(Duration.ofMillis(1000))
                .map(l -> new Price().setPrice(count.incrementAndGet()))
                .onOverflow().drop();
    }

}

@ApplicationScoped
public static class Consumer {

    List<Price> prices = new CopyOnWriteArrayList<>();

    @Incoming("from-amqp")
    public void consume(JsonObject p) {             // <2>
        Price price = p.mapTo(Price.class);         // <3>
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

Messages coming from AMQP contains an instance of {{ javadoc('io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-amqp') }}

``` java
{{ insert('amqp/inbound/AmqpMetadataExample.java', 'code') }}
```

## Acknowledgement

When a Reactive Messaging `Message` associated with an AMQP Message is
acknowledged, it informs the broker that the message has been
*accepted*.

## Failure Management

If a message produced from an AMQP message is *nacked*, a failure
strategy is applied. The AMQP connector supports six strategies:

-   `fail` - fail the application; no more AMQP messages will be
    processed (default). The AMQP message is marked as rejected.

-   `accept` - this strategy marks the AMQP message as *accepted*. The
    processing continues ignoring the failure. Refer to the [accepted
    delivery state
    documentation](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-accepted).

-   `release` - this strategy marks the AMQP message as *released*. The
    processing continues with the next message. The broker can redeliver
    the message. Refer to the [released delivery state
    documentation](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-released).

-   `reject` - this strategy marks the AMQP message as rejected. The
    processing continues with the next message. Refer to the [rejected
    delivery state
    documentation](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-rejected).

-   `modified-failed` - this strategy marks the AMQP message as
    *modified* and indicates that it failed (with the `delivery-failed`
    attribute). The processing continues with the next message, but the
    broker may attempt to redeliver the message. Refer to the [modified
    delivery state
    documentation](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified)

-   `modified-failed-undeliverable-here` - this strategy marks the AMQP
    message as *modified* and indicates that it failed (with the
    `delivery-failed` attribute). It also indicates that the application
    cannot process the message, meaning that the broker will not attempt
    to redeliver the message to this node. The processing continues with
    the next message. Refer to the [modified delivery state
    documentation](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified)

## Configuration Reference


{{ insert('../../../target/connectors/smallrye-amqp-incoming.md') }}

You can also pass any property supported by the [Vert.x AMQP
client](https://vertx.io/docs/vertx-amqp-client/java/) as attribute.

To use an existing *address* or *queue*, you need to configure the
`address`, `container-id` and, optionally, the `link-name` attributes.
For example, if you have an Apache Artemis broker configured with:

``` xml
<queues>
    <queue name="people">
        <address>people</address>
        <durable>true</durable>
        <user>artemis</user>
    </queue>
</queues>
```

You need the following configuration:

``` properties
mp.messaging.incoming.people.connector=smallrye-amqp
mp.messaging.incoming.people.durable=true
mp.messaging.incoming.people.address=people
mp.messaging.incoming.people.container-id=people
```

You may need to configure the `link-name` attribute, if the queue name
is not the channel name:

``` properties
mp.messaging.incoming.people-in.connector=smallrye-amqp
mp.messaging.incoming.people-in.durable=true
mp.messaging.incoming.people-in.address=people
mp.messaging.incoming.people-in.container-id=people
mp.messaging.incoming.people-in.link-name=people
```

## Receiving Cloud Events

The AMQP connector supports [Cloud Events](https://cloudevents.io/).
When the connector detects a *structured* or *binary* Cloud Events, it
adds a {{ javadoc('io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata') }}
into the metadata of the `Message`. `IncomingCloudEventMetadata`
contains accessors to the mandatory and optional Cloud Event attributes.

If the connector cannot extract the Cloud Event metadata, it sends the
Message without the metadata.

### Binary Cloud Events

For `binary` Cloud Events, **all** mandatory Cloud Event attributes must
be set in the AMQP application properties, prefixed by `cloudEvents:`
(as mandated by the [protocol
binding](https://github.com/cloudevents/spec/blob/v1.0.1/amqp-protocol-binding.md)).
The connector considers headers starting with the `cloudEvents:` prefix
but not listed in the specification as extensions. You can access them
using the `getExtension` method from `IncomingCloudEventMetadata`.

The `datacontenttype` attribute is mapped to the `content-type` header
of the record.

### Structured Cloud Events

For `structured` Cloud Events, the event is encoded in the record’s
value. Only JSON is supported, so your event must be encoded as JSON in
the record’s value.

Structured Cloud Event must set the `content-type` header of the record
to `application/cloudevents+json; charset=UTF-8`. The message body must
be a valid JSON object containing at least all the mandatory Cloud
Events attributes.

If the record is a structured Cloud Event, the created Message’s payload
is the Cloud Event `data`.
