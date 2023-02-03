# Receiving messages from JMS

The JMS Connector retrieves JMS Message and maps each of them into
Reactive Messaging `Messages`.

## Example

Let’s imagine you have a `jakarta.jms.ConnectionFactory` bean exposed and
connected to your JMS server. Don’t forget that it’s required to use the
JMS connector.

Configure your application to receive JMS messages on the `prices`
channel as follows:

```properties
mp.messaging.incoming.prices.connector=smallrye-jms
```

!!!note
    You don’t need to set the destination. By default, it uses the channel
    name (`prices`). You can configure the `destination` attribute to
    override it.

!!!note
    By default the connector uses a `queue`. You can configure it to use a
    `topic` by setting `destination-type=topic`.

Then, your application receives `Message<Double>`. You can consume the
payload directly:

``` java
{{ insert('jms/inbound/JmsPriceConsumer.java') }}
```

Or, you can retrieve the `Message<Double>`:

``` java
{{ insert('jms/inbound/JmsPriceMessageConsumer.java') }}
```

## Deserialization

The content of the incoming JMS message is mapped to a Java object.

By default it extracts the JMS Message *body* as a `java.lang.Object`.
This can be changed by setting, in the incoming JMS Message:

1.  The `_classname` property

2.  the `JMSType`

The value must be a fully qualified class name. The connector then load
the associated class.

!!!note
    The connector loads the associated `Class` using the `TCCL` and if not
    found, the classloader used to load the connector.

If the target type is a primitive type ort `String`, the resulting
message contains the mapped payload.

If the target type is a class, the object is built using included JSON
deserializer (JSON-B and Jackson provided OOB, for more details see
[Serde](serde)), from the `JMSType`. If not, the default behavior is
used (Java deserialization).

## Inbound Metadata

Messages coming from JMS contains an instance of {{ javadoc('io.smallrye.reactive.messaging.jms.IncomingJmsMessageMetadata', True, 'io.smallrye.reactive/smallrye-reactive-messaging-jms') }}  in the metadata.

``` java
{{ insert('jms/inbound/JmsMetadataExample.java', 'code') }}
```

## Acknowledgement

When the Reactive Messaging `Message` gets acknowledged, the associated
JMS Message is acknowledged. As JMS acknowledgement is blocking, this
acknowledgement is delegated to a worker thread.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-jms-incoming.md') }}
