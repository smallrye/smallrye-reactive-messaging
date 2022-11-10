# Configuring the schema used for Pulsar channels

Pulsar messages are stored with payloads as unstructured byte array.
A Pulsar **schema** defines how to serialize structured data to the raw message bytes
The **schema** is applied in producers and consumers to write and read with an enforced data structure.
It serializes data into raw bytes before they are published to a topic and deserializes the raw bytes before they are delivered to consumers.

Pulsar uses a schema registry as a central repository to store the registered schema information,
which enables producers/consumers to coordinate the schema of a topic's messages through brokers.
By default the Apache BookKeeper is used to store schemas.

Pulsar API provides built-in schema information for a number of
[primitive types](https://pulsar.apache.org/docs/3.0.x/schema-understand#primitive-type)
and [complex types](https://pulsar.apache.org/docs/3.0.x/schema-understand#complex-type) such as Key/Value, Avro and Protobuf.

The Pulsar Connector allows specifying the schema as a primitive type using the `schema` property:

```properties
mp.messaging.incoming.prices.connector=smallrye-pulsar
mp.messaging.incoming.prices.schema=INT32

mp.messaging.outgoing.prices-out.connector=smallrye-pulsar
mp.messaging.outgoing.prices-out.schema=DOUBLE
```

If the value for the `schema` property matches a [Schema Type](https://javadoc.io/doc/org.apache.pulsar/pulsar-client-api/latest/org/apache/pulsar/common/schema/SchemaType.html)
a simple schema will be created with that type and will be used for that channel.

The Pulsar Connector allows configuring complex schema types by providing `Schema` beans through CDI, identified with the `@Identifier` qualifier.

For example the following bean provides an Avro schema and a Key/Value schema:

```java
{{ insert('pulsar/configuration/PulsarSchemaProvider.java') }}
```

To configure the incoming channel `users` with defined schema, you need to set the `schema` property to the identifier of the schema `user-schema`:

```properties
mp.messaging.incoming.users.connector=smallrye-pulsar
mp.messaging.incoming.users.schema=user-schema
```

If no `schema` property is found, the connector looks for `Schema` beans identified with the channel name.
For example, the outgoing channel `a-channel` will use the key/value schema.

```properties
mp.messaging.outgoing.a-channel.connector=smallrye-pulsar
```

If no schema information is provided incoming channels will use `Schema.AUTO_CONSUME()`, whereas outgoing channels will use `Schema.AUTO_PRODUCE_BYTES()` schemas.

