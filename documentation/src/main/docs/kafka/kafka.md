# Apache Kafka Connector

The Kafka connector adds support for Kafka to Reactive Messaging. With
it you can receive Kafka Records as well as write `message` into Kafka.

[Apache Kafka](https://kafka.apache.org/) is a popular distributed
streaming platform. It lets you:

-   Publish and subscribe to streams of records, similar to a message
    queue or enterprise messaging system.

-   Store streams of records in a fault-tolerant durable way.

-   Process streams of records as they occur.

The Kafka cluster stores streams of *records* in categories called
*topics*. Each record consists of a *key*, a *value*, and a *timestamp*.

For more details about Kafka, check the
[documentation](https://kafka.apache.org/intro).

## Using the Kafka Connector

To use the Kafka Connector, add the following dependency to your
project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-kafka</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-kafka`.

So, to indicate that a channel is managed by this connector you need:

```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-kafka

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-kafka
```
