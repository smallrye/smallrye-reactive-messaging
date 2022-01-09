# Connectors

Reactive Messaging can handle messages generated from within the
application but also interact with remote *brokers*. Reactive Messaging
Connectors interacts with these remote *brokers* to retrieve messages
and send messages using various protocols and technology.

Each connector handles to a specific technology. For example, a Kafka
Connector is responsible for interacting with Kafka, while an MQTT
Connector is responsible for MQTT interactions.

## Connector name

Each connector has a name. This name is referenced by the application to
indicate that this connector manages a specific channel.

For example, the SmallRye Kafka Connector is named: `smallrye-kafka`.

## Inbound and Outbound connectors

Connector can:

1.  retrieve messages from a remote broker (inbound)

2.  send messages to a remove broker (outbound)

A connector can, of course, implement both directions.

Inbound connectors are responsible for:

1.  Getting messages from the remote broker,

2.  Creating a Reactive Messaging `Message` associated with the
    retrieved message.

3.  Potentially associating technical metadata with the message. It
    includes unmarshalling the payload.

4.  Associating an acknowledgment callback to acknowledge the incoming
    message when the Reactive Messaging message is
    processed/acknowledged.

!!!important "Reactive matters"
    The first step should follow the reactive streams principle: uses
    non-blocking technology, respects downstream requests.

Outbound connectors are responsible for:

1.  Receiving Reactive Messaging `Message` and transform it into a
    structure understood by the remote broker. It includes marshaling
    the payload.

2.  If the `Message` contains outbound metadata (metadata set during the
    processing to influence the outbound structure and routing), taking
    them into account.

3.  Sending the message to the remote broker.

4.  Acknowledging the Reactive Messaging `Message` when the broker has
    accepted/acknowledged the message.

## Configuring connectors

Applications need to configure the connector used by expressing which
channel is managed by which connector. Non-mapped channels are local /
in-memory.

To configure connectors, you need to have an implementation of
MicroProfile Config. If you don’t have one, add an implementation of
MicroProfile Config in your *classpath*, such as:

``` xml
<dependency>
  <groupId>io.smallrye.config</groupId>
  <artifactId>smallrye-config</artifactId>
  <version>{{ attributes['smallrye-config-version'] }}</version>
</dependency>
```

Then edit the application configuration, generally
`src/main/resources/META-INF/microprofile-config.properties`.

The application configures the connector with a set of properties
structured as follows:

    mp.messaging.[incoming|outgoing].[channel-name].[attribute]=[value]

For example:

    mp.messaging.incoming.dummy-incoming-channel.connector=dummy
    mp.messaging.incoming.dummy-incoming-channel.attribute=value
    mp.messaging.outgoing.dummy-outgoing-channel.connector=dummy
    mp.messaging.outgoing.dummy-outgoing-channel.attribute=value

You configure each channel (both incoming and outgoing) individually.

The `[incoming|outgoing]` segment indicates the direction.

-   an `incoming` channel consumes data from a message broker or
    something producing data. It’s an inbound interaction. It relates to
    methods annotated with an `@Incoming` using the same channel name.

-   an `outgoing` consumes data from the application and forwards it to
    a message broker or something consuming data. It’s an outbound
    interaction. It relates to methods annotated with an `@Outgoing`
    using the same channel name.

The `[channel-name]` is the name of the channel. If the channel name
contains a `.` (dot), you would need to use `"` (double-quote) around
it. For example, to configure the `dummy.incoming.channel` channel, you
would need:

    mp.messaging.incoming."dummy.incoming.channel".connector=dummy
    mp.messaging.incoming."dummy.incoming.channel".attribute=value

The `[attribute]=[value]` sets a specific connector attribute to the
given value. Attributes depend on the used connector. So, refer to the
connector documentation to check the supported attributes.

The `connector` attribute must be set for each mapped channel and
indicates the name of the connector responsible for the channel.

Here is an example of a channel using an MQTT connector, consuming data
from a MQTT broker, and a channel using a Kafka connector (writing data
to Kafka):

```properties
# Configure the incoming health channel
mp.messaging.incoming.health.topic=neo
mp.messaging.incoming.health.connector=smallrye-mqtt
mp.messaging.incoming.health.host=localhost
mp.messaging.incoming.health.broadcast=true

# Configure outgoing data channel
mp.messaging.outgoing.data.connector=smallrye-kafka
mp.messaging.outgoing.data.bootstrap.servers=localhost:9092
mp.messaging.outgoing.data.key.serializer=org.apache.kafka.common.serialization.StringSerializer
mp.messaging.outgoing.data.value.serializer=io.vertx.kafka.client.serialization.JsonObjectSerializer
mp.messaging.outgoing.data.acks=1
```

!!!important
    To use a connector, you need to add it to your *CLASSPATH*. Generally,
    adding the dependency to your project is enough. Then, you need to know
    the connector’s name and set the `connector` attribute for each channel
    managed by this connector.


# Connector attribute table

In the connector documentation, you will find a table listing the
attribute supported by the connector. Be aware that attributes for
inbound and outbound interactions may be different.

These tables contain the following entries:

1.  The name of the attribute, and potentially an *alias*. The name of
    the attribute is used with the
    `mp.messaging.[incoming|outgoing].[channel-name].[attribute]=[value]`
    syntax (the `attribute` segment). The *alias* (if set) is the name
    of a global MicroProfile Config property that avoids having to
    configure the attribute for each managed channel. For example, to
    set the location of your Kafka broker globally, you can use the
    `kafka.bootstrap.servers` alias.

2.  The description of the attribute, including the type.

3.  Whether that attribute is mandatory. If so, it fails the
    deployment if not set

4.  The default value, if any.
