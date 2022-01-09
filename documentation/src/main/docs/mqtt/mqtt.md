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

