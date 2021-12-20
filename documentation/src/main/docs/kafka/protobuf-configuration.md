# Using Google Protobuf serializer/deserializer

If you are using [Protocol
Buffers](https://developers.google.com/protocol-buffers/)
serializer/deserializer, please note the following configuration
properties.

## For [Confluent](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html#serdes-and-formatter-protobuf) Schema Registry

Confluent protobuf library is `io.confluent:kafka-protobuf-serializer`.
Note that this library is not available in Maven Central, you need to
use the [Confluent Maven
repository](https://docs.confluent.io/clients-kafka-java/current/overview.html).

### Consumer

| Property                                                       | Recommended value                                                  |
|----------------------------------------------------------------|--------------------------------------------------------------------|
| value.deserializer                                             | io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer  |
| schema.registry.url                                            | [http://{your_host}:{your_port}/](http://{your_host}:{your_port}/) |
| mp.messaging.incoming.\[channel\].specific.protobuf.value.type | your.package.DomainObjectKey$Key                                   |
| mp.messaging.incoming.\[channel\].specific.protobuf.key.type   | your.package.DomainObjectValue$Value                               |

Example:

    mp.messaging.incoming.[channel].value.deserializer=io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer
    mp.messaging.incoming.[channel].schema.registry.url=http://{your_host}:{your_port}/
    mp.messaging.incoming.[channel].specific.protobuf.value.type=your.package.DomainObjectKey$Key
    mp.messaging.incoming.[channel].specific.protobuf.key.type=your.package.DomainObjectValue$Value

### Producer

| Property            | Recommended value                                                  |
|---------------------|--------------------------------------------------------------------|
| value.serializer    | io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer    |
| schema.registry.url | [http://{your_host}:{your_port}/](http://{your_host}:{your_port}/) |

Example:

    mp.messaging.outgoing.[channel].value.serializer=io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
    mp.messaging.outgoing.[channel].schema.registry.url=http://{your_host}:{your_port}/
