# Sending data with Camel

You can use the Camel connector to send data to almost any type of
system.

To keep this document focused on the Camel connector, we use the Camel
File component. However, the connector can be used with any Camel
component.

## Example

Let’s imagine you want to write generated prices into files. Configure
your application to write the messages from the `prices` channel into a
files as follows:

```properties
mp.messaging.outgoing.prices.connector=smallrye-camel # <1>
mp.messaging.outgoing.prices.endpoint-uri=file:prices/?fileName=${date:now:yyyyMMddssSS}.txt&charset=utf-8 # <2>
```
1.  Sets the connector for the `prices` channel
2.  Configure the `endpoint-uri` to write into files in the `prices`
    directory

!!!important
    Depending on your implementation of MicroProfile Reactive Messaging, the
    `$` may need to be escaped as follows: `$${...}`

Then, your application must send `Message<String>` to the `prices`
channel. It can use `String` payloads as in the following snippet:

``` java
{{ insert('camel/outbound/CamelPriceProducer.java') }}
```

Or, you can send `Message<Double>`:

``` java
{{ insert('camel/outbound/CamelPriceMessageProducer.java') }}
```

## Serialization

The serialization is handled by the Camel component. Refer to the Camel
documentation to check which payload type is supported by the component.

## Outbound Metadata

When sending `Messages`, you can add an instance
of {{ javadoc('io.smallrye.reactive.messaging.camel.OutgoingExchangeMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-camel') }}
to the message metadata. You can then influence how the outbound Camel
`Exchange` is created, for example by adding properties:

``` java
{{ insert('camel/outbound/CamelOutboundMetadataExample.java', 'code') }}
```

## Acknowledgement

The incoming messages are acknowledged when the underlying Camel
exchange completes. If the exchange fails, the message is nacked.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-camel-outgoing.md') }}

