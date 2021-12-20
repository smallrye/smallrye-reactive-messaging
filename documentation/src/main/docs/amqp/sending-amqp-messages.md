# Sending messages to AMQP

The AMQP connector can write Reactive Messaging `Messages` as AMQP
Messages.

## Example

Let’s imagine you have an AMQP broker (such as [Apache ActiveMQ
Artemis](https://activemq.apache.org/components/artemis/)) running, and
accessible using the `amqp:5672` address (by default it would use
`localhost:5672`). Configure your application to send the messages from
the `prices` channel as AMQP Message as follows:

```properties
amqp-host=amqp  # <1>
amqp-port=5672  # <2>
amqp-username=my-username # <3>
amqp-password=my-password # <4>

mp.messaging.outgoing.prices.connector=smallrye-amqp # <5>
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
    You don’t need to set the *address*. By default, it uses the channel
    name (`prices`). You can configure the `address` attribute to override
    it.

Then, your application must send `Message<Double>` to the `prices`
channel. It can use `double` payloads as in the following snippet:

``` java
{{ insert('amqp/outbound/AmqpPriceProducer.java') }}
```

Or, you can send `Message<Double>`:

``` java
{{ insert('amqp/outbound/AmqpPriceMessageProducer.java') }}
```

## Serialization

When receiving a `Message<T>`, the connector convert the message into an
AMQP Message. The payload is converted to the AMQP Message *body*.

| `T`                                                                                                                                                                | AMQP Message Body                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| primitive types or `String`                                                                                                                                        | AMQP Value with the payload                                                                                                                                              |
| `Instant` or `UUID`                                                                                                                                                | AMQP Value using the corresponding AMQP Type                                                                                                                             |
| [`JsonObject`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonObject.html) or [`JsonArray`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonArray.html) | AMQP Data using a binary content. The `content-type` is set to `application/json`                                                                                        |
| `io.vertx.mutiny.core.buffer.Buffer`                                                                                                                               | AMQP Data using a binary content. No `content-type` set                                                                                                                  |
| Any other class                                                                                                                                                    | The payload is converted to JSON (using a Json Mapper). The result is wrapped into AMQP Data using a **binary** content. The `content-type` is set to `application/json` |

If the message payload cannot be serialized to JSON, the message is
*nacked*.

## Outbound Metadata

When sending `Messages`, you can add an instance of
{{ javadoc('io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-amqp') }}
to influence how the message is going to be sent to AMQP. For example, you
can configure the subjects, properties:

``` java
{{ insert('amqp/outbound/AmqpOutboundMetadataExample.java', 'code') }}
```

## Dynamic address names

Sometimes it is desirable to select the destination of a message
dynamically. In this case, you should not configure the address inside
your application configuration file, but instead, use the outbound
metadata to set the address.

For example, you can send to a dynamic address based on the incoming
message:

``` java
{{ insert('amqp/outbound/AmqpOutboundDynamicAddressExample.java', 'code') }}
```

!!!note
    To be able to set the address per message, the connector is using an
    *anonymous sender*.

## Acknowledgement

By default, the Reactive Messaging `Message` is acknowledged when the
broker acknowledged the message. When using routers, this
acknowledgement may not be enabled. In this case, configure the
`auto-acknowledgement` attribute to acknowledge the message as soon as
it has been sent to the router.

If an AMQP message is rejected/released/modified by the broker (or
cannot be sent successfully), the message is nacked.

## Back Pressure and Credits

The back-pressure is handled by AMQP *credits*. The outbound connector
only requests the amount of allowed credits. When the amount of credits
reaches 0, it waits (in a non-blocking fashion) until the broker grants
more credits to the AMQP sender.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-amqp-outgoing.md') }}


You can also pass any property supported by the [Vert.x AMQP
client](https://vertx.io/docs/vertx-amqp-client/java/) as attribute.

## Using existing destinations

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
mp.messaging.outgoing.people.connector=smallrye-amqp
mp.messaging.outgoing.people.durable=true
mp.messaging.outgoing.people.address=people
mp.messaging.outgoing.people.container-id=people
```

You may need to configure the `link-name` attribute, if the queue name
is not the channel name:

``` properties
mp.messaging.outgoing.people-out.connector=smallrye-amqp
mp.messaging.outgoing.people-out.durable=true
mp.messaging.outgoing.people-out.address=people
mp.messaging.outgoing.people-out.container-id=people
mp.messaging.outgoing.people-out.link-name=people
```

To use a `MULTICAST` queue, you need to provide the *FQQN*
(Fully-qualified queue name) instead of just the name of the queue:

``` properties
mp.messaging.outgoing.people-out.connector=smallrye-amqp
mp.messaging.outgoing.people-out.durable=true
mp.messaging.outgoing.people-out.address=foo
mp.messaging.outgoing.people-out.container-id=foo

mp.messaging.incoming.people-out.connector=smallrye-amqp
mp.messaging.incoming.people-out.durable=true
mp.messaging.incoming.people-out.address=foo::bar # Note the syntax: address-name::queue-name
mp.messaging.incoming.people-out.container-id=bar
mp.messaging.incoming.people-out.link-name=people
```

More details about the AMQP Address model can be found on [the Artemis
documentation](https://activemq.apache.org/components/artemis/documentation/2.0.0/address-model.html).

## Sending Cloud Events

The AMQP connector supports [Cloud Events](https://cloudevents.io/). The
connector sends the outbound record as Cloud Events if:

-   the message metadata contains an
    `io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata`
    instance,

-   the channel configuration defines the `cloud-events-type` and
    `cloud-events-source` attributes.

You can create
`io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata` instances
using:

``` java
{{ insert('amqp/outbound/AmqpCloudEventProcessor.java') }}
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
{{ insert('amqp/outbound/AmqpCloudEventMultipleProcessors.java') }}
```

By default, the connector sends the Cloud Events using the *binary*
format. You can write *structured* Cloud Events by setting the
`cloud-events-mode` to `structured`. Only JSON is supported, so the
created records had its `content-type` header set to
`application/cloudevents+json; charset=UTF-8`

!!!note
    you can disable the Cloud Event support by setting the `cloud-events`
    attribute to `false`

