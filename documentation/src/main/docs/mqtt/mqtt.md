# MQTT Connector

The MQTT connector adds support for MQTT to Reactive Messaging.

It lets you receive messages from an MQTT server or broker as well as
send MQTT messages. The MQTT connector is based on the [Vert.x MQTT
Client](https://vertx.io/docs/vertx-mqtt/java/#_vert_x_mqtt_client).

## Introduction

[MQTT](http://mqtt.org/) is a machine-to-machine (M2M)/"Internet of
Things" connectivity protocol. It was designed as an extremely
lightweight publish/subscribe messaging transport.

The MQTT Connector allows consuming messages from MQTT as well as
sending MQTT messages.

## Using the MQTT connector

To you the MQTT Connector, add the following dependency to your project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-mqtt</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-mqtt`.

So, to indicate that a channel is managed by this connector you need:
```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-mqtt

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-mqtt
```

## Protocol version

The connector supports both MQTT 3.1.1 and MQTT 5.0. The protocol
version is selected per channel through the `version` attribute:

```properties
# MQTT 3.1.1 (default)
mp.messaging.incoming.prices.version=4

# MQTT 5.0
mp.messaging.incoming.prices.version=5
```

When `version=5` is used, the connector exposes the additional MQTT 5.0
features described in the rest of this section:

- Connect-time properties: `session-expiry-interval`, `receive-maximum`,
  `topic-alias-maximum`.
- Last Will properties: `will-topic`, `will-payload`, `will-qos`,
  `will-retain`, plus the MQTT 5.0 specific
  `will-content-type`, `will-response-topic` and `will-delay-interval`.
  Both `will-topic` and `will-payload` must be set together, otherwise
  the connector fails to start.
- Subscription options: `no-local`, `retain-as-published`,
  `retain-handling`.
- Per-message properties (User Properties, Content-Type, Response Topic,
  Correlation Data, Message Expiry Interval, Payload Format Indicator)
  available on inbound and outbound messages.

!!!note
    These options are accepted but ignored by the broker when
    `version=4` is used.

