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

Attach a
`io.smallrye.reactive.messaging.mqtt.SendingMqttMessageMetadata` to an
outgoing `Message` to override the topic, the QoS and the retain flag,
and to carry the MQTT 5.0 message properties:

```java
SendingMqttMessageMetadata metadata = SendingMqttMessageMetadataBuilder.builder()
        .withTopic("prices")
        .withQos(MqttQoS.AT_LEAST_ONCE)
        .withContentType("application/json")
        .withUserProperty("source", "sensor-1")
        .build();

return MqttMessage.of(metadata, payload);
```

The MQTT 5.0 properties are only sent when the channel is configured
with `mqtt-version=5`.

## MQTT 5.0 request/response

MQTT 5.0 carries request/response interactions with the `Response Topic`
and `Correlation Data` message properties. `MqttMessage.ofResponse(...)`
builds the reply to such a request: it sends the message on the topic
the requester asked for, and copies the correlation data so that the
requester can match the reply with its request.

``` java
{{ insert('mqtt/outbound/MqttRequestResponseHandler.java') }}
```

```properties
mp.messaging.incoming.requests.connector=smallrye-mqtt
mp.messaging.incoming.requests.host=mqtt
mp.messaging.incoming.requests.mqtt-version=5
mp.messaging.incoming.requests.topic=requests

mp.messaging.outgoing.responses.connector=smallrye-mqtt
mp.messaging.outgoing.responses.host=mqtt
mp.messaging.outgoing.responses.mqtt-version=5
# The topic attribute is unused: the response is sent on the
# `Response Topic` of the incoming request.
```

If the incoming message has no `Response Topic`, for example because the
requester used MQTT 3.1.1, `ofResponse` throws an
`IllegalArgumentException`.

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
