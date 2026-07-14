# Sending messages to MQTT

The MQTT Connector can write Reactive Messaging `Messages` as MQTT
Message.

## Example

Let’s imagine you have a MQTT server/broker running, and accessible
using the `mqtt:1883` address (by default it would use
`localhost:1883`). Configure your application to write the messages from
the `prices` channel into a MQTT Messages as follows:

```properties
mp.messaging.outgoing.prices.type=smallrye-mqtt
mp.messaging.outgoing.prices.host=mqtt
mp.messaging.outgoing.prices.port=1883
```

1.  Sets the connector for the `prices` channel
2.  Configure the broker/server host name.
3.  Configure the broker/server port. 1883 is the default.

!!!note
    You don’t need to set the MQTT topic. By default, it uses the channel
    name (`prices`). You can configure the `topic` attribute to override it.
    NOTE: It is generally recommended to set the `client-id`. By default,
    the connector is generating a unique `client-id`.


Then, your application must send `Message<Double>` to the `prices`
channel. It can use `double` payloads as in the following snippet:

``` java
{{ insert('mqtt/outbound/MqttPriceProducer.java') }}
```

Or, you can send `Message<Double>`:

``` java
{{ insert('mqtt/outbound/MqttPriceMessageProducer.java') }}
```

## Serialization

The `Message` sent to MQTT can have various payload types:

-   [`JsonObject`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonObject.html):
    JSON string encoded as `byte[]`

-   [`JsonArray`](https://vertx.io/docs/apidocs/io/vertx/core/json/JsonArray.html):
    JSON string encoded as `byte[]`

-   `java.lang.String` and Java primitive types: `toString` encoded as
    `byte[]`

-   `byte[]`

-   complex objects: The objects are encoded to JSON and passed as
    `byte[]`

## Outbound Metadata

You can attach
`io.smallrye.reactive.messaging.mqtt.SendingMqttMessageMetadata` to any
outgoing `Message` to override the topic, QoS, retain flag and — for
MQTT 5.0 — to attach arbitrary `MqttProperties` to the publish:

```java
MqttProperties properties = new MqttProperties();
properties.add(new MqttProperties.UserProperty("source", "sensor-1"));

return MqttMessage.of("prices", payload, MqttQoS.AT_LEAST_ONCE, false, properties);
```

The `MqttMessage` interface provides several factory methods that cover
the common cases:

| Factory                                                                  | Use case                                                    |
|--------------------------------------------------------------------------|-------------------------------------------------------------|
| `MqttMessage.of(payload)`                                                | Publish on the channel's default topic                      |
| `MqttMessage.of(topic, payload)`                                         | Override the topic                                          |
| `MqttMessage.of(topic, payload, qos)`                                    | Override topic and QoS                                      |
| `MqttMessage.of(topic, payload, qos, retain)`                            | Override topic, QoS and retain flag                         |
| `MqttMessage.of(topic, payload, qos, retain, MqttProperties properties)` | Add MQTT 5.0 properties                                     |
| `MqttMessage.of(topic, payload, MqttProperties properties)`              | Add MQTT 5.0 properties keeping default QoS and retain flag |
| `MqttMessage.ofResponse(request, payload, qos[, properties])`            | Build a reply to an MQTT 5.0 request (see below)            |

## MQTT 5.0 request/response

MQTT 5.0 supports request/response interactions through the
`Response Topic` and `Correlation Data` message properties. When the
inbound channel is configured with `version=5`, the connector exposes
those values via the incoming `MqttMessage`, and the
`MqttMessage.ofResponse(...)` helper builds a reply that targets the
correct topic and propagates the correlation data automatically.

The following example handles a request and replies on the topic
indicated by the requester, attaching two MQTT 5.0 User Properties to
the response:

``` java
{{ insert('mqtt/outbound/MqttRequestResponseHandler.java') }}
```

Configuration:

```properties
mp.messaging.incoming.requests.connector=smallrye-mqtt
mp.messaging.incoming.requests.host=mqtt
mp.messaging.incoming.requests.version=5
mp.messaging.incoming.requests.topic=requests

mp.messaging.outgoing.responses.connector=smallrye-mqtt
mp.messaging.outgoing.responses.host=mqtt
mp.messaging.outgoing.responses.version=5
# The topic attribute is not used: ofResponse() overrides it with the
# Response Topic carried by the incoming request.
```

`MqttMessage.ofResponse(request, payload, qos, properties)`:

- Reads the `Response Topic` property from `request` and uses it as
  destination topic of the reply. If the request has no Response Topic
  (for example because it was sent in MQTT 3.1.1) an
  `IllegalArgumentException` is thrown.
- Copies the `Correlation Data` of the request, if any, into the reply
  so that the original requester can match request and response.
- Does not mutate the `MqttProperties` instance passed by the caller —
  it builds an internal copy with `Correlation Data` added.

The two-argument variant `MqttMessage.ofResponse(request, payload, qos)`
behaves the same way but uses an empty set of additional properties.

## Acknowledgement

MQTT acknowledgement depends on the QoS level. The message is
acknowledged when the broker indicated the successful reception of the
message (or immediately if the level of QoS does not support
acknowledgment).

If a MQTT message cannot be sent to the broker, the message is `nacked`.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-mqtt-outgoing.md') }}


The MQTT connector is based on the [Vert.x MQTT
client](https://vertx.io/docs/vertx-mqtt/java/#_vert_x_mqtt_client). So
you can pass any attribute supported by this client.

!!!important
    A single instance of `MqttClient` and a single connection is used for
    each `host` / `port` / `server-name` / `client-id`. This client is
    reused for both the inbound and outbound connectors.
