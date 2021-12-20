# Emitter and Channels

It is not rare to combine in a single application imperative parts
(Jax-RS, regular CDI *beans*) and reactive parts (*beans* with
`@Incoming` and `@Outgoing` annotations). In these case, it’s often
required to send *messages* from the imperative part to the reactive
part. In other words, send messages to channels handled by reactive
messaging and how can you retrieve messages.

## Emitter and @Channel

To send *things* (payload or `Message`) from imperative code to a
specific channel you need to use:

1.  the {{ javadoc('org.eclipse.microprofile.reactive.messaging.Channel', True) }} annotations
2.  the {{ javadoc('org.eclipse.microprofile.reactive.messaging.Emitter', True) }} type

The `@Channel` lets you indicate to which channel you are going to send
your payloads or messages. The `Emitter` is the object to use to send
these payloads or messages.

``` java
{{ insert('emitter/MyImperativeBean.java', 'intro') }}
```

The `Emitter` class takes a type parameter. It’s the type of payload.
Even if you want to send `Messages`, the type is the payload type.

!!!important
    You must have a `@Incoming("prices")` somewhere in your application
    (meaning a method consuming messages transiting on the channel
    `prices`), or an outbound connector configured to manage the `prices`
    channel (`mp.messaging.outgoing.prices...`)

## Sending payloads

Sending payloads is done as follows:

``` java
{{ insert('emitter/EmitterExamples.java', 'payload') }}
```

When sending a payload, the emitter returns a `CompletionStage`. This
`CompletionStage` gets completed once the message created from the
payload is acknowledged:

``` java
{{ insert('emitter/EmitterExamples.java', 'cs') }}
```

If the processing fails, the `CompletionStage` gets completed
exceptionally (with the reason of the nack).

## Sending messages

You can also send `Messages`:

``` java
{{ insert('emitter/EmitterExamples.java', 'message') }}
```

When sending a `Message`, the emitter does not return a
`CompletionStage`, but you can pass the ack/nack callback, and be called
when the message is acked/nacked.

``` java
{{ insert('emitter/EmitterExamples.java', 'message-ack') }}
```

Sending messages also let you pass metadata.

``` java
{{ insert('emitter/EmitterExamples.java', 'message-meta') }}
```

Metadata can be used to propagate some context objects with the message.

## Overflow management

When sending messages from imperative code to reactive code, you must be
aware of back-pressure. Indeed, messages sent using the emitter and
stored in a *queue*. If the consumer does not process the messages
quickly enough, this queue can become a memory hog and you may even run
out of memory.

To control what need to happen when the queue becomes out of control,
use the {{ javadoc('org.eclipse.microprofile.reactive.messaging.OnOverflow') }} annotation. `@OnOverflow` lets you configure:

-   the maximum size of the queue (default is 256)
-   what needs to happen when this size is reached (fail, drop...)

``` java
{{ insert('emitter/EmitterExamples.java', 'overflow') }}
```

The supported strategies are:

-   `OnOverflow.Strategy.BUFFER` - use a buffer to store the elements
    until they are consumed. If the buffer is full, a failure is
    propagated (and the thread using the emitted gets an exception)

-   `OnOverflow.Strategy.UNBOUNDED_BUFFER` - use an unbounded buffer to
    store the elements

-   `OnOverflow.Strategy.DROP` - drops the most recent value if the
    downstream can’t keep up. It means that new value emitted by the
    emitter are ignored.

-   `OnOverflow.Strategy.FAIL` - propagates a failure in case the
    downstream can’t keep up.

-   `OnOverflow.Strategy.LATEST` - keeps only the latest value, dropping
    any previous value if the downstream can’t keep up.

-   `OnOverflow.Strategy.NONE` - ignore the back-pressure signals
    letting the downstream consumer to implement a strategy.

### Defensive emission

Having an emitter injected into your code does not guarantee that
someone is ready to consume the message. For example, a subscriber may
be connecting to a remote broker. If there are no subscribers, using the
`send` method will throw an exception.

The `emitter.hasRequests()` method indicates that a subscriber
subscribes to the channel and requested items. So, you can wrap your
emission with:

``` java
if (emitter.hasRequests()) {
    emitter.send("hello");
}
```

If you use the `OnOverflow.Strategy.DROP`, you can use the `send` method
even with no subscribers nor demands. The message will be nacked
immediately.

## Retrieving channels

You can use the `@Channel` annotation to inject in your bean the
underlying stream. Note that in this case, you will be responsible for
the subscription:

``` java
{{ insert('emitter/ChannelExamples.java', 'channel') }}
```

!!!important
    You must have a `@Outgoing("my-channel")` somewhere in your application
    (meaning a method generating messages transiting on the channel
    `my-channel`), or an inbound connector configured to manage the `prices`
    channel (`mp.messaging.incoming.prices...`)

Injected *channels* merge all the matching *outgoing* - so if you have
multiple `@Outgoing("out")`, `@Inject @Channel("out")` gets all the
messages.

If your injected *channel* receives *payloads* (`Multi<T>`), it
acknowledges the message automatically, and support multiple
subscribers. If you injected *channel* receives `Message`
(`Multi<Message<T>>`), you will be responsible for the acknowledgement
and broadcasting.

## Emitter and @Broadcast

When using an `Emitter`, you can now `@Broadcast` what is emitted to all
subscribers.

Here is an example of emitting a price with two methods marked
`@Incoming` to receive the broadcast:

``` java
{{ insert('emitter/EmitterExamples.java', 'broadcast') }}
```

For more details see [@Broadcast](broadcast.md)
documentation.

## Mutiny Emitter

If you prefer to utilize `Uni` in all your code, there is now a
`MutinyEmitter` that will return `Uni<Void>` instead of `void`.

``` java
{{ insert('emitter/MutinyExamples.java', 'uni') }}
```

There’s also the ability to block on sending the event to the emitter.
It will only return from the method when the event is acknowledged, or
nacked, by the receiver:

``` java
{{ insert('emitter/MutinyExamples.java', 'uni-await') }}
```

And if you don’t need to worry about the success or failure of sending
an event, you can `sendAndForget`:

``` java
{{ insert('emitter/MutinyExamples.java', 'uni-forget') }}
```
