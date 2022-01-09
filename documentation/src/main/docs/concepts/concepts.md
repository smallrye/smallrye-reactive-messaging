When dealing with event-driven or data streaming applications, there are
a few concepts and vocabulary to introduce.

## Messages, Payload, Metadata

A `Message` is an *envelope* around a `payload`. Your application is
going to receive, process, and send `Messages`.

Your application’s logic can generate these `Messages` or receive them
from a message broker. They can also be consumed by your application or
sent to a message broker.

{{ image('../../images/messages.png', 'An application can receive a message, process it and send a resulting message') }}


In Reactive Messaging, `Message` are represented by the {{ javadoc('io.eclipse.microprofile.reactive.messaging.Message') }} interface.
Each `Message<T>` contains a *payload* of type `<T>`. This payload can be retrieved using `message.getPayload()`:

``` java
{{ insert('messages/MessageExamples.java', 'message') }}
```

As you can see in the previous snippet, `Messages` can also have
*metadata*. *Metadata* is a way to extend messages with additional data.
It can be metadata related to the message broker
(like {{ javadoc('io.smallrye.reactive.messaging.kafka.api.KafkaMessageMetadata') }}), or contain operational data (such as tracing metadata), or business-related data.

!!! note
    When retrieving metadata, you get an `Optional` as it may not be  present.

!!! tip
    Metadata is also used to influence the outbound dispatching (how the  message will be sent to the broker).

## Channels and Streams

Inside your application, `Messages` transit on *channel*. A *channel* is
a virtual destination identified by a name.

{{ image('../../images/channels.png', 'The application is a set of channels') }}

SmallRye Reactive Messaging connects the component to the channel they read and to the channel they populate.
The resulting structure is a  stream: Messages flow between components through channels.

!!! note "What about Reactive Streams?"
    You may wonder why *Reactive Messaging* has *Reactive* in the name. The
    Messaging part is kind of obvious. The Reactive part comes from the
    streams that are created by binding components. These streams are
    [Reactive Streams](https://www.reactive-streams.org/). They follow the
    subscription and request protocol and implement back-pressure. It also
    means that [Connectors](#connectors) are intended to use non-blocking IO
    to interact with the various message brokers.


## Connectors

Your application is interacting with messaging brokers or event backbone  using *connectors*.
A *connector* is a piece of code that connects to a broker and:

1.  subscribe/poll/receive messages from the broker and propagate them to the application

2.  send/write/dispatch messages provided by the application to the broker

Connectors are configured to map incoming messages to a specific
*channel* (consumed by the application) and collect outgoing messages
sent to a specific channel. These collected messages are sent to the
external broker.

{{ image('../../images/connectors.png', 'Connectors manages the communication between the application and the brokers') }}

Each connector is dedicated to a specific technology.
For example, a  Kafka Connector only deals with Kafka.

You don’t necessarily need a connector.
When your application does not  use connectors, everything happens *in-memory*, and the streams are created by chaining methods altogether.
Each chain is still a reactive  stream and enforces the back-pressure protocol.
When you don’t use  connectors, you need to make sure the chain is complete, meaning it  starts with a message source, and it ends with a sink.
In other words,  you need to generate messages from within the application (using a method with only `@Outgoing`, or an `Emitter`) and consume the messages
from within the application (using a method with only `@Incoming` or using an unmanaged stream).

