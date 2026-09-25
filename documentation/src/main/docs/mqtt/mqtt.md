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

The connector speaks MQTT 3.1.1 by default, and MQTT 5.0 when the
channel sets `mqtt-version=5`:

```properties
mp.messaging.incoming.prices.mqtt-version=5
```

MQTT 5.0 enables the connect properties `session-expiry-interval`,
`receive-maximum`, `topic-alias-maximum` and `authentication-method`,
the subscription options `no-local`, `retain-as-published`,
`retain-handling` and `subscription-identifier`, and the per-message
properties described in the inbound and outbound metadata sections.
These attributes are accepted but ignored by the broker when the channel
stays on MQTT 3.1.1.

## Last Will and Testament

The broker publishes the will message when the client disconnects
without a proper DISCONNECT packet. It is configured on the channel:

```properties
mp.messaging.incoming.prices.will-topic=status/prices
mp.messaging.incoming.prices.will-payload=offline
mp.messaging.incoming.prices.will-qos=1
mp.messaging.incoming.prices.will-retain=true
```

The will message is sent by the broker as soon as `will-topic` and
`will-payload` are set. They must be set together: the connector fails
to start otherwise, since the broker never sends a will message missing
one of them.

With `mqtt-version=5`, the will message also accepts
`will-content-type`, `will-response-topic` and `will-delay-interval`,
the latter being the number of seconds the broker waits before
publishing it.
