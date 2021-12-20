# Retrieving data using Camel

Camel provides many components. To keep this documentation focused on
the integration with Camel, we use the [File
component](https://camel.apache.org/components/latest/file-component.html).
This component let use read files from a directory. So the connector
configured with this component creates a `Message` for each file located
in the directory. As soon as a file is dropped in the directory, a new
`Message` is created.

## Example

Let’s imagine you want to read the files from the `orders` directory and
send them to the `files` channel. Configuring the Camel connector to
gets the *file* from this directory only requires 2 properties:

```properties
mp.messaging.incoming.files.connector=smallrye-camel # <1>
mp.messaging.incoming.files.endpoint-uri=file:orders/?delete=true&charset=utf-8 # <2>
```
1.  Sets the connector for the `files` channel
2.  Configures the `endpoint-uri`

Then, your application receives `Message<GenericFile<File>>`.

!!!note
    The Camel File component produces
    `org.apache.camel.component.file.GenericFile` instances. You can
    retrieve the actual `File` using `getFile()`.

You can consume the payload directly:

``` java
{{ insert('camel/inbound/CamelFileConsumer.java') }}
```

You can also retrieve the `Message<GenericFile<File>>`:

``` java
{{ insert('camel/inbound/CamelFileMessageConsumer.java') }}
```

## Deserialization

Each Camel component is producing specific objects. As we have seen, the
File component produces `GenericFile`.

Refer to the component documentation to check which type is produced.

## Inbound Metadata

Messages coming from Camel contains an instance of
{{ javadoc('io.smallrye.reactive.messaging.camel.IncomingExchangeMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-camel') }}
in the metadata.

``` java
{{ insert('camel/inbound/IncomingCamelMetadataExample.java') }}
```

This object lets you retrieve the Camel `Exchange`.

## Failure Management

If a message produced from a Camel exchange is *nacked*, a failure
strategy is applied. The Camel connector supports 3 strategies:

-   `fail` - fail the application, no more MQTT messages will be
    processed. (default) The offset of the record that has not been
    processed correctly is not committed.
-   `ignore` - the failure is logged, but the processing continue.

In both cases, the `exchange` is marked as rollback only and the nack
reason is attached to the exchange.

## Configuration Reference


{{ insert('../../../target/connectors/smallrye-camel-incoming.md') }}
