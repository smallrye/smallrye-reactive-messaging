# Using Apache Avro serializer/deserializer

If you are using [Apache Avro](https://avro.apache.org/)
serializer/deserializer, please note the following configuration
properties.

## For [Confluent](https://docs.confluent.io/current/schema-registry/serdes-develop/serdes-avro.html) Schema Registry

Confluent Avro library is `io.confluent:kafka-avro-serializer`. Note
that this library is not available in Maven Central, you need to use the
[Confluent Maven
repository](https://docs.confluent.io/clients-kafka-java/current/overview.html).

### Consumer

| Property             | Recommended value                                                  |
|----------------------|--------------------------------------------------------------------|
| value.deserializer   | io.confluent.kafka.serializers.KafkaAvroDeserializer               |
| schema.registry.url  | [http://{your_host}:{your_port}/](http://{your_host}:{your_port}/) |
| specific.avro.reader | true                                                               |

Example:

    mp.messaging.incoming.[channel].value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
    mp.messaging.incoming.[channel].schema.registry.url=http://{your_host}:{your_port}/
    mp.messaging.incoming.[channel].specific.avro.reader=true

### Producer

| Property            | Recommended value                                                  |
|---------------------|--------------------------------------------------------------------|
| value.serializer    | io.confluent.kafka.serializers.KafkaAvroSerializer                 |
| schema.registry.url | [http://{your_host}:{your_port}/](http://{your_host}:{your_port}/) |

Example:

    mp.messaging.outgoing.[channel].value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
    mp.messaging.outgoing.[channel].schema.registry.url=http://{your_host}:{your_port}/

## For [Apicurio](https://www.apicur.io/registry/) Registry 1.x

Apicurio Registry 1.x Avro library is
`io.apicurio:apicurio-registry-utils-serde`.

The configuration properties listed here are meant to be used with the
Apicurio Registry 1.x client library and Apicurio Registry 1.x server.

### Consumer

| Property                                   | Recommended value                                                        |
|--------------------------------------------|--------------------------------------------------------------------------|
| value.deserializer                         | io.apicurio.registry.utils.serde.AvroKafkaDeserializer                   |
| apicurio.registry.url                      | [http://{your_host}:{your_port}/api](http://{your_host}:{your_port}/api) |
| apicurio.registry.avro-datum-provider      | io.apicurio.registry.utils.serde.avro.DefaultAvroDatumProvider           |
| apicurio.registry.use-specific-avro-reader | true                                                                     |

Example:

    mp.messaging.incoming.[channel].value.deserializer=io.apicurio.registry.utils.serde.AvroKafkaDeserializer
    mp.messaging.incoming.[channel].apicurio.registry.url=http://{your_host}:{your_port}/api
    mp.messaging.incoming.[channel].apicurio.registry.avro-datum-provider=io.apicurio.registry.utils.serde.avro.DefaultAvroDatumProvider
    mp.messaging.incoming.[channel].apicurio.registry.use-specific-avro-reader=true

### Producer

| Property              | Recommended value                                                        |
|-----------------------|--------------------------------------------------------------------------|
| value.serializer      | io.apicurio.registry.utils.serde.AvroKafkaSerializer                     |
| apicurio.registry.url | [http://{your_host}:{your_port}/api](http://{your_host}:{your_port}/api) |

To automatically register schemas with the registry, add:

| Property                    | Value                                                           |
|-----------------------------|-----------------------------------------------------------------|
| apicurio.registry.global-id | io.apicurio.registry.utils.serde.strategy.GetOrCreateIdStrategy |

Example:

    mp.messaging.outgoing.[channel].value.serializer=io.apicurio.registry.utils.serde.AvroKafkaSerializer
    mp.messaging.outgoing.[channel].apicurio.registry.url=http://{your_host}:{your_port}/api
    mp.messaging.outgoing.[channel].apicurio.registry.global-id=io.apicurio.registry.utils.serde.strategy.GetOrCreateIdStrategy

## For [Apicurio](https://www.apicur.io/registry/) Registry 2.x

Apicurio Registry 2.x Avro library is
`io.apicurio:apicurio-registry-serdes-avro-serde`.

The configuration properties listed here are meant to be used with the
Apicurio Registry 2.x client library and Apicurio Registry 2.x server.

### Consumer

| Property                                   | Recommended value                                                                                  |
|--------------------------------------------|----------------------------------------------------------------------------------------------------|
| value.deserializer                         | io.apicurio.registry.serde.avro.AvroKafkaDeserializer                                              |
| apicurio.registry.url                      | [http://{your_host}:{your_port}/apis/registry/v2](http://{your_host}:{your_port}/apis/registry/v2) |
| apicurio.registry.use-specific-avro-reader | true                                                                                               |

Example:

    mp.messaging.incoming.[channel].value.deserializer=io.apicurio.registry.serde.avro.AvroKafkaDeserializer
    mp.messaging.incoming.[channel].apicurio.registry.url=http://{your_host}:{your_port}/apis/registry/v2
    mp.messaging.incoming.[channel].apicurio.registry.use-specific-avro-reader=true

### Producer

| Property              | Recommended value                                                                                  |
|-----------------------|----------------------------------------------------------------------------------------------------|
| value.serializer      | io.apicurio.registry.serde.avro.AvroKafkaSerializer                                                |
| apicurio.registry.url | [http://{your_host}:{your_port}/apis/registry/v2](http://{your_host}:{your_port}/apis/registry/v2) |

To automatically register schemas with the registry, add:

| Property                        | Value |
|---------------------------------|-------|
| apicurio.registry.auto-register | true  |

Example:

    mp.messaging.outgoing.[channel].value.serializer=io.apicurio.registry.serde.avro.AvroKafkaSerializer
    mp.messaging.outgoing.[channel].apicurio.registry.url=http://{your_host}:{your_port}/apis/registry/v2
    mp.messaging.outgoing.[channel].apicurio.registry.auto-register=true
