# The processor pattern using Camel

Using the processor pattern, you can consume on a channel using a Camel
component, and produce on a channel using another Camel component. In
that case, the headers present in the incoming metadata will be
forwarded in the outgoing metadata.

## Example

Let’s imagine you want to read messages from a Nats subject, process it
and produce a message on a Kafka topic.

```properties
mp.messaging.incoming.mynatssubject.connector=smallrye-camel # <1>
mp.messaging.incoming.mynatssubject.endpoint-uri=nats:mynatssubject # <2>
mp.messaging.outgoing.mykafkatopic.connector=smallrye-camel # <3>
mp.messaging.outgoing.mykafkatopic.endpoint-uri=kafka:mykafkatopic# <4>

camel.component.nats.servers=127.0.0.1:5555 # <5>
camel.component.kafka.brokers=127.0.0.1:9092 # <6>
```
1.  Sets the connector for the `mynatssubject` channel
2.  Configures the `endpoint-uri` for nats subject
3.  Sets the connector for the `mykafkatopic` channel
4.  Configures the `endpoint-uri` for the kafka topic
5.  Sets the URL of the nats server to use
6.  Sets the URL of a kafka broker to use

``` java
{{ insert('camel/processor/CamelProcessor.java') }}
```
