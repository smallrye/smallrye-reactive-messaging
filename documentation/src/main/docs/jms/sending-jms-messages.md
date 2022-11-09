# Sending messages to JMS

The JMS Connector can send Reactive Messaging `Messages` as JMS Message.

## Example

Let’s imagine you have a `jakarta.jms.ConnectionFactory` bean exposed and
connected to your JMS server. Don’t forget that it’s required to use the
JMS connector.

Configure your application to write the messages from the `prices`
channel into a JMS Message as follows:

```properties
mp.messaging.outgoing.prices.connector=smallrye-jms
```

!!!note
    You don’t need to set the destination. By default, it uses the channel
    name (`prices`). You can configure the `destination` attribute to
    override it.

!!!note
    By default the connector uses a `queue`. You can configure it to use a
    `topic` by setting `destination-type=topic`.

Then, your application must send `Message<Double>` to the `prices`
channel. It can use `double` payloads as in the following snippet:

``` java
{{ insert('jms/outbound/JmsPriceProducer.java') }}
```

Or, you can send `Message<Double>`:

``` java
{{ insert('jms/outbound/JmsPriceMessageProducer.java') }}
```

## Serialization

The connector serializes the incoming message payload into the body of
the outgoing JMS Message.

If the payload is a `String` or a primitive type, the payload is encoded
as `String` and the `JMSType` is set to the target class. The
`_classname` property is also set. The JMS Message is a `TextMessage`.

If the payload is a `byte[]`, it’s passed as `byte[]` in a JMS
`BytesMessage`.

Otherwise, the payload is encoded using included JSON serializer (JSON-B
and Jackson provided OOB, for more details see [Serde](serde)). The
`JMSType` is set to the target class. The `_classname` property is also
set. The JMS Message is a `TextMessage`.

For example, the following code serialize the produced `Person` using
JSON-B.

``` java
@Incoming("...")
@Outgoing("my-channel")
public Person sendToJms(...) {
  // ...
  return new Person("bob", 42);
}
```

It requires that the `Person` class can be serialized to JSON. The
classname is passed in the `JMSType` property and `_classname` property.

## Outbound Metadata

When sending `Messages`, you can add an instance of {{ javadoc('io.smallrye.reactive.messaging.jms.OutgoingJmsMessageMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-jms') }}
to influence how the message is going to be written to JMS.

``` java
{{ insert('jms/outbound/JmsOutboundMetadataExample.java', 'code') }}
```

The metadata allow adding properties but also override the destination.

## Acknowledgement

Once the JMS message is sent to the JMS server, the message is
acknowledged. Sending a JMS message is a blocking operation. So, sending
is done on a worker thread.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-jms-outgoing.md') }}

