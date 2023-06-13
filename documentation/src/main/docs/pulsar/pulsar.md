# Apache Pulsar Connector

The Pulsar connector adds support for Apache Pulsar to Reactive Messaging. With
it you can consume Pulsar Messages as well as produce *message* into Pulsar.

[Apache Pulsar](https://pulsar.apache.org/) an open-source, distributed messaging
and streaming platform built for the cloud. For more details about Pulsar, check the
[documentation](https://pulsar.apache.org/docs/next/concepts-overview/).

Pulsar implements the publish-subscribe pattern.
Producers publish *messages* to *topic*s.
Consumers create *subscription*s to those topics to receive and process incoming messages,
and send *acknowledgments* to the broker when processing is finished.

When a *subscription* is created, Pulsar retains all messages, even if the consumer is disconnected.
The retained messages are discarded only when a consumer acknowledges that all these messages are processed successfully.

A Pulsar *message* consists of

- **value**, payload data that message contains, while Pulsar messages contain payloads as unstructured byte array,
a **schema** is applied to write and read with an enforced data structure
- **key** of type string, used for partitioning
- **properties**, optional key/value map
- **topic**, the topic that the message is published to
- **producer name**, the name of the producer who produces the message
- **message ID**, the ID assigned by bookies to a message as soon as the message is persistently stored
- **sequence ID**, the ID assigned by producers, indicating its order in that sequence
- **publish time**, timestamp automatically added by the producer
- **event time**, optional timestamp added by the application

A Pulsar cluster consists of
- One or more **broker**s, which are stateless components
- A **metadata store** for maintaining topic metadata, schema, coordination and cluster configuration.
By default, a Zookeeper cluster is used, except the standalone mode
- A ensemble of **bookies** used for persistent storage of messages


## Using the Pulsar Connector

To use the Pulsar Connector, add the following dependency to your
project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-pulsar</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-pulsar`.

So, to indicate that a channel is managed by this connector you need:

```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-pulsar

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-pulsar
```
