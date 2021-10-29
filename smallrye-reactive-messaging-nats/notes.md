# Implementation Notes

***@cescoffier***:

> Is there a "good" non-blocking API

- ***Requirement***: Implementation must be non blocking

- According to the [nats.java#connecting:4](https://github.com/nats-io/nats.java#connecting) the client is capable of asynchronous communication.
```java
Options options = new Options.Builder().server(Options.DEFAULT_URL).connectionListener(handler).build();
Nats.connectAsynchronously(options, true);
```
- They have marked asynchronous connections as experimental
- According to [nats.java#listening](https://github.com/nats-io/nats.java#listening-for-incoming-messages) there is an async callback thread based way to receive messages

```java
Dispatcher d = nc.createDispatcher((msg) -> {
    String response = new String(msg.getData(), StandardCharsets.UTF_8);
    ...
});

d.subscribe("subject");
```

> How back-pressure is handled and how can we adapt it to reactive messaging

- "NATS automatically handles a slow consumer. If a client is not processing messages quick enough, the NATS server cuts it off" [source](https://docs.nats.io/nats-server/nats_admin/slow_consumers)
- There is an exception thrown client side that serves as a warning that a consumer is slow [source](https://docs.nats.io/nats-server/nats_admin/slow_consumers)
- A consumer can limit the incoming queue size [source](https://docs.nats.io/developing-with-nats/events/slow#detect-a-slow-consumer-and-check-for-dropped-messages)
- The client throws an exception that can be handled when it is force disconnected by the server [source](https://github.com/nats-io/nats.java/blob/main/src/main/java/io/nats/client/ErrorListener.java)
- After researching back-pressure I found the following options
1. Control the Producer
   1. NATS [does not recommend](https://github.com/nats-io/nats.docs/blob/master/nats-server/nats_admin/slow_consumers.md#handling-slow-consumers) metering the publisher
   2. One can issue a periodic request/reply to match subscriber rates
2. Create a bounded buffer
   1. NatsConnectionReader and NatsConnectionWriter [implement buffering](https://github.com/nats-io/nats.java/blob/4fe5e0a6e97cb1628ad508302b3946dcde9552b3/src/main/java/io/nats/client/impl/NatsConnectionReader.java)
   2. I could write a configurable FIFO queue to further buffer messages
3. Drop Messages
   1. This seems like a bad idea since the messages aren't ack'd (see below)
- Naive approach: just yolo it and punt handling back pressure for now
- I'm still not really sure how to handle backpressure, any ideas?

> How is acknowledgment/negative-acknowledgment handled,
- The nats.io Java library has a method for n/acking
- Unless the connection is a Jetstream connection [n/acking is a NO-OP](https://github.com/nats-io/nats.java/blob/main/src/main/java/io/nats/client/impl/NatsMessage.java)
- Since they didn't implement it
  - would it be desirable to override their implementation and implement it?
  - or is it fine if left as is

**client/impl/NatsMessage.java**
```java
...
    @Override
    public void ack() {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    @Override
    public void ackSync(Duration d) throws InterruptedException, TimeoutException {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }

    @Override
    public void nak() {
        // do nothing. faster. saves checking whether a message is jetstream or not
    }
...
```


> Is the consumption per message or per batch
- I would want the consumption to be per message
- I think mirroring how the NATS Java library handles it would be nice

**Hypothetical Example Usage**:
- References:
  - [The NatsSub Example](https://github.com/nats-io/nats.java/blob/main/src/examples/java/io/nats/examples/NatsSub.java)
  - [Smallrye Reactive Messaging mqtt-quickstat](https://github.com/smallrye/smallrye-reactive-messaging/blob/main/examples/mqtt-quickstart/src/main/java/acme/Receiver.java)

```java
import io.nats.client.Message;
import imaginary.message.format.Deserialize;
...
    @Incoming("get.example.model.*")                                            //[1]
    public CompletionStage<Void> consume(NatsMessage<Message> message) {        //[2]
        if (msg.hasHeaders()) {                                                 //[3]
            ...
        }

        String subject = message.getSubject();                                  //[4]

        byte[] data = message.getData();                                        //[5]

        MyDTO dto = Deserialize.fromBytes(data , MyDTO.class);                  //[6]

        ...

    }
```


1. In nats topics are called subjects
   1. users should be able to set raw subjects as strings or have them mapped from microprofile config
2. Everything else in this library has a ```Message<T>``` container by convention
   1. ```NatsMessage<T>``` would implement ```org.eclipse.microprofile.reactive.messaging.Message<T>```
   2. The Message object from nats.io could be contained by NatsMessage
   3. Nats Message objects have ```{subject: String, header: Map<k,v> , data: byte[]}```
      1. Link to [Nats Message interface](https://github.com/nats-io/nats.java/blob/fa93ff728602f0e75e1094921371d16be1fc1338/src/main/java/io/nats/client/Message.java#L32)
   4. **idea**: the ability to pick a deserializer
      1. That way one could do ```NatsMessage<MyDTO>``` and have it be automatically mapped
3. Messages have optional headers
   1. If you map a DTO then maybe there could be a second parameter that takes the header object
4. The subject is the topic name, would be the raw form of [1]
5. Nats protocol is binary and serialization format is left to the user
   1. Can use plain UTF8 Strings, JSON, JSONB, etc
   2. NATS Streaming [uses protobuf](https://docs.nats.io/developing-with-nats-streaming/protocol)
      1. According to the Streaming Server's [repository README](https://github.com/nats-io/nats-streaming-server) **it is deprecated**
         1. In favor of [Jetstream](https://docs.nats.io/jetstream/jetstream)
6. Like the **idea** above, providing JSON/B deserialization out of the box would be helpful
   1. Maybe an integration with Jackson or JSONB as other contributions have



***@ozangunalp***

> Maybe it goes without saying but having a container util for running the nats server in tests is very important.
- ***Requirement***: set up dev container for nats server in tests.
- [Nats Docker](https://docs.nats.io/nats-server/nats_docker)

```yaml
version: "3.5"
services:
  nats:
    image: nats
    ports:
      - "8222:8222"
    networks: ["nats"]
networks:
  nats:
    name: nats
```

- This project uses Test Containers
  - There is no prebuilt NATS Test Container interface
  - There is a [merge-able open PR](https://github.com/testcontainers/testcontainers-java/pull/3888) on the [Test Containers Repo](https://github.com/testcontainers/testcontainers-java/) 🎉
  - I can bug the mainers to review and merge the PR
  - Or I can write a generic one, all it has to do is run the docker image as above.
- There is a [Java Nats Server Runner](https://github.com/nats-io/java-nats-server-runner) that is [used by the library](https://github.com/nats-io/nats.java/blob/60625c33a63bf8727d1631a64ee41451973469f9/src/test/java/io/nats/client/NatsTestServer.java) to run NATS
  - It relies on having the nats-server binary in the path
  - would need to figure out how to dynamically acquire and package nats-server for tests

- I guess there's three options here, any preference?

  - Option 1: Get the NATS Test Containers PR merged and use that
  - Option 2: Use GenericContainer Test Container
  - Option 3: Use the official Java Nats Server Runner
```java
@Container
public GenericContainer nats = new GenericContainer(DockerImageName.parse("nats:latest"))
                                        .withExposedPorts(8222);
```


> Backpressure needs to be handled both ways: For fast publishers and slow consumers.
- ***Requirement***: apply bi directional backpressure
  - Research what backpressure is and how to handle it
- See the answer above, I'm not too sure how to handle back pressure, any ideas?

> How to check for connections status for health checks?
- ***Requirement***: Contribution must expose health check
- The [NatsConnection](https://github.com/nats-io/nats.java/blob/main/src/main/java/io/nats/client/impl/NatsConnection.java) manages connections and exposes a getStatus() method which returns a [Status](https://github.com/nats-io/nats.java/blob/fa93ff728602f0e75e1094921371d16be1fc1338/src/main/java/io/nats/client/support/Status.java#L19) object representing the current state of the connection.
- NATS Exposes a bunch of [Monitoring Endpoints](https://docs.nats.io/nats-server/configuration/monitoring)

