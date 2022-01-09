# Development Model

Reactive Messaging proposes a CDI-based programming model to implement event-driven applications.
Following the CDI principles, _beans_ are forming the main building block of your application.
Reactive Messaging provides a set of annotations and types to implement beans that generate, consume or process messages.

## @Incoming and @Outgoing

Reactive Messaging provides two main annotations:

- {{ javadoc('org.eclipse.microprofile.reactive.messaging.Incoming', true) }} - indicates the consumed channel
- {{ javadoc('org.eclipse.microprofile.reactive.messaging.Outgoing', true) }} - indicates the populated channel

These annotations are used on *methods*:

``` java
{{ insert('beans/MessageProcessingBean.java') }}
```

!!! note
Reactive Messaging beans can either be in the *application* scope (`@ApplicationScoped`) or dependent scope (`@Dependent`).

Manipulating messages can be cumbersome.
When you are only interested in the payload, you can use the following syntax: The following code is equivalent to the snippet from above:

``` java
{{ insert('beans/PayloadProcessingBean.java') }}
```

!!! important
You should not call methods annotated with `@Incoming` and/or
`@Outgoing` directly from your code. They are invoked by the framework.
Having user code invoking them would not have the expected outcome.


SmallRye Reactive Messaging automatically binds matching `@Outgoing` to
`@Incoming` to form a chain:

{{ image('../../images/chain.png', 'A chain of components') }}

If we consider the following code:

``` java
{{ insert('beans/Chain.java', 'chain') }}
```

It would generate the following chain:

``` text
generate --> [ source ] --> process --> [ sink ] --> consume
```

Methods annotated with `@Incoming` or `@Outgoing` don’t have to be in the same *bean* (*class*).
You can distribute them among a set of beans.
Remote interactions are also possible when using connectors.

Methods annotated with:

- only `@Outgoing` are used to generate messages or payloads
- only `@Incoming` are used to consume messages or payloads
- both `@Incoming` and `@Outgoing` are used to process messages or payloads; or transform the stream

## Creating messages

Messages are envelopes around payload.
They are the vehicle.
While manipulating payload is convenient, messages let you add metadata, handle acknowledgement...

Creating `Messages` is done using the {{ javadoc('io.eclipse.microprofile.reactive.messaging.Message') }} interface directly:

``` java
{{ insert('messages/MessageExamples.java', 'creation') }}
```

You can also create new instance of `Message` from an existing one:

``` java
{{ insert('messages/MessageExamples.java', 'copy') }}
```

!!! note "Acknowledgement?"
Acknowledgement is an important part of messaging systems. This will be
covered in the [acknowledgement](acknowledgement.md)
section.

!!! note "Connector Metadata"
Most connectors are providing metadata to let you extract technical
details about the message, but also customize the outbound dispatching.

## Generating Messages

To produce messages to a channel, you need to use the `@Outgoing`
annotation. This annotation takes a single parameter: the name of the
populated channel.

### Generating messages synchronously

You can generate messages synchronously. In this case, the method is
called for every *request* from the downstream:

``` java
{{ insert('generation/GenerationExamples.java', 'message-sync') }}
```

!!! note "Requests?"
Reactive Messaging connects components to build a reactive stream.
In a  reactive stream, the emissions are controlled by the consumer
(downstream) indicating to the publisher (upstream) how many items it
can consume. With this protocol, the consumers are never flooded.


### Generating messages using CompletionStage

You can also return a `CompletionStage` / `CompletableFuture`.
In this  case, Reactive Messaging waits until the `CompletionStage` gets  completed before calling it again.

For instance, this signature is useful to poll messages from a source  using an asynchronous client:

``` java
{{ insert('generation/GenerationExamples.java', 'message-cs') }}
```

### Generating messages using Uni

You can also return a [Uni](https://smallrye.io/smallrye-mutiny/#_uni_and_multi) instance.
In  this case, Reactive Messaging waits until the `Uni` emits its item  before calling it again.

This signature is useful when integrating asynchronous clients providing  a Mutiny API.

``` java
{{ insert('generation/GenerationExamples.java', 'message-uni') }}
```

### Generating Reactive Streams of messages

Instead of producing the message one by one, you can return the stream
directly. If you have a data source producing Reactive Streams
`Publisher` (or sub-types, such as `Multi`), this is the signature you are looking for:

``` java
{{ insert('generation/GenerationExamples.java', 'message-stream') }}
```

In this case, the method is called once to retrieve the `Publisher`.

## Generating Payloads

Instead of `Message`, you can produce *payloads*. In this case, Reactive
Messaging produces a simple message from the *payload* using
`Message.of`.

### Generating payload synchronously

You can produce *payloads* synchronously. The framework calls the method
upon request and create `Messages` around the produced payloads.

``` java
{{ insert('generation/GenerationExamples.java' , 'payload-sync') }}
```

### Generating payload using CompletionStage

You can also return `CompletionStage` or `CompletableFuture`. For
example, if you have an asynchronous client returning `CompletionStage`,
you can use it as follows, to poll the data one by one:

``` java
{{ insert('generation/GenerationExamples.java' , 'payload-cs') }}
```

### Generating payload by producing Unis

You can also return a `Uni` if you have a client using Mutiny types:

``` java
{{ insert('generation/GenerationExamples.java' , 'payload-uni') }}
```

### Generating Reactive Streams of payloads

Finally, you can return a `Publisher` (or a sub-type such as a `Multi`):

``` java
{{ insert('generation/GenerationExamples.java' , 'payload-stream') }}
```

In this case, Reactive Messaging calls the method only once to retrieve
the `Publisher`.

## Consuming Messages

To consume messages from a channel, you need to use the `@Incoming`
annotation. This annotation takes a single parameter: the name of the
consumed channel.

Because `Messages` must be acknowledged, consuming messages requires
returning *asynchronous results* that would complete when the incoming
message get acknowledged.

For example, you can receive the `Message`, process it and return the
acknowledgement as result:

``` java
{{ insert('consumption/ConsumptionExamples.java', 'message-cs') }}'
```

You can also return a `Uni` if you need to implement more complicated
processing:

``` java
{{ insert('consumption/ConsumptionExamples.java', 'message-uni') }}
```

## Consuming Payloads

Unlike consuming messages, consuming payloads support both synchronous
and asynchronous consumption.

For example, you can consume a payload as follows:

``` java
{{ insert('consumption/ConsumptionExamples.java', 'payload-sync') }}
```

In this case, you don’t need to deal with the acknowledgement yourself.
The framework acknowledges the incoming message (that wrapped the
payload) once your method returns successfully.

If you need to achieve asynchronous actions, you can return a
`CompletionStage` or a `Uni`:

``` java
{{ insert('consumption/ConsumptionExamples.java', 'payload-cs') }}
```

``` java
{{ insert('consumption/ConsumptionExamples.java', 'payload-uni') }}
```

In these 2 cases, the framework acknowledges the incoming message when
the returned construct gets *completed*.

## Processing Messages

You can process `Message` both synchronously or asynchronously.
This  later case is useful when you need to execute an asynchronous action during your processing such as invoking a remote service.

Do process `Messages` synchronously uses:

``` java
{{ insert('processing/ProcessingExamples.java', 'process-message') }}
```

This method transforms the `int` payload to a `String`, and wraps it
into a `Message`.

'''important "Using `Message.withX` methods"
You may be surprised by the usage of `Message.withX` methods. It allows
metadata propagation as the metadata would be copied from the incoming
message and so dispatched to the next method.

You can also process `Messages` asynchronously:

``` java
{{ insert('processing/ProcessingExamples.java', 'process-message-cs') }}
```

Or using Mutiny:

``` java
{{ insert('processing/ProcessingExamples.java', 'process-message-uni') }}
```

In general, you want to create the new `Message` from the incoming one.
It enables metadata propagation and post-acknowledgement. For this, use
the `withX` method from the `Message` class returning a new `Message`
instance but copy the *content* (metadata, ack/nack...).

## Processing payloads

If you don’t need to manipulate the envelope, you can process payload
directly either synchronously or asynchronously:

``` java
{{ insert('processing/ProcessingExamples.java', 'process-payload') }}
```

``` java
{{ insert('processing/ProcessingExamples.java', 'process-payload-cs') }}
```

``` java
{{ insert('processing/ProcessingExamples.java', 'process-payload-uni') }}
```

!!! note "What about metadata?"
With these methods, the metadata are automatically propagated.

## Processing streams

The previous processing method were taking single `Message` or payload.
Sometimes you need more advanced manipulation. For this, SmallRye
Reactive Messaging lets you process the stream of `Message` or the
stream of payloads directly:

``` java
{{ insert('processing/StreamExamples.java', 'processing-stream-message')}}
```

``` java
{{ insert('processing/StreamExamples.java', 'processing-stream-payload')}}
```

You can receive either a (Reactive Streams) `Publisher`, a
`PublisherBuilder` or (Mutiny) `Multi`. You can return any subclass of
`Publisher` or a `Publisher` directly.

!!!important
These signatures do not support metadata propagation. In the case of a
stream of `Message`, you need to propagate the metadata manually. In the
case of a stream of payload, propagation is not supported, and incoming
metadata are lost.
