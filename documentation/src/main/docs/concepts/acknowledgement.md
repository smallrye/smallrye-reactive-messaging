# Acknowledgement

Acknowledgment is an essential concept in messaging. A message is
acknowledged when its processing or reception has been successful. It
allows the broker to move to the next message.

How acknowledgment is used, and the exact behavior in terms of retry and
resilience depends on the broker. For example, for Kafka, it would
commit the offset. For AMQP, it would inform the broker that the message
has been *accepted*.

Reactive Messaging supports acknowledgement. The default acknowledgement
depends on the method signature. Also, the acknowledgement policy can be
configured using the `@Acknowledgement` annotation.

## Chain of acknowledgment

If we reuse this example:

``` java
{{ insert('beans/Chain.java', 'chain') }}
```

The framework automatically acknowledges the message received from the
`sink` channel when the `consume` method returns. As a consequence, the
message received by the `process` method is acknowledged, and so on. In
other words, it creates a chain of acknowledgement - from the outbound
channel to the inbound channel.

When using connectors to receive and consume messages, the outbound
connector acknowledges the messages when they are dispatched
successfully to the broker. The acknowledgment chain would, as a result,
acknowledges the inbound connector, which would be able to send an
acknowledgment to the broker.

This chain of acknowledgment is automatically implemented when
processing payloads.

## Acknowledgment when using Messages

When using `Messages`, the user controls the acknowledgment, and so the
chain is not formed automatically. It gives you more flexibility about
when and how the incoming messages are acknowledged.

If you create a `Message` using the `with` method, is copy the
acknowledgment function from the incoming message:

``` java
{{ insert('ack/MessageAckExamples.java', 'with-payload') }}
```

To have more control over the acknowledgment, you can create a brand new
`Message` and pass the acknowledgment function:

``` java
{{ insert('ack/MessageAckExamples.java', 'message-creation') }}
```

However, you may need to create the acknowledgment chain, to acknowledge
the incoming message:

``` java
{{ insert('ack/MessageAckExamples.java', 'process') }}
```

To trigger the acknowledgment of the incoming message, use the `ack()`
method. It returns a `CompletionStage`, receiving `null` as value when
the acknowledgment has completed.

## Acknowledgment when using streams

When transforming streams of `Message`, the acknowledgment is delegated
to the user. It means that it’s up to the user to acknowledge the
incoming messages:

``` java
{{ insert('ack/StreamAckExamples.java', 'message') }}
```

In the previous example, we only generate a single message per incoming
message so that we can use the `with` method. It becomes more
sophisticated when grouping incoming messages or when each incoming
message produces multiple messages.

In the case of a stream of payloads, the default strategy acknowledges
the incoming messages before being processed by the method (regardless
of the outcome).

``` java
{{ insert('ack/StreamAckExamples.java', 'payload') }}
```

## Controlling acknowledgement

The  {{ javadoc('org.eclipse.microprofile.reactive.messaging.Acknowledgment') }}
annotation lets you customize the default strategy presented in the
previous sections. The `@Acknowledgement` annotation takes a *strategy*
as parameter. Reactive Messaging proposed 4 strategies:

-   `POST_PROCESSING` - the acknowledgement of the incoming message is
    executed once the produced message is acknowledged.

-   `PRE_PROCESSING` - the acknowledgement of the incoming message is
    executed before the message is processed by the method.

-   `MANUAL` - the acknowledgement is doe by the user.

-   `NONE` - No acknowledgment is performed, neither manually or
    automatically.

It is recommended to use `POST_PROCESSING` as it guarantees that the
full processing has completed before acknowledging the incoming message.
However, sometimes it’s not possible, and this strategy is not available
if you manipulate streams of `Messages` or payloads.

The `PRE_PROCESSING` strategy can be useful to acknowledge a message
early in the process:

``` java
{{ insert('ack/PreAckExamples.java', 'pre') }}
```

It cuts the acknowledgment chain, meaning that the rest of the
processing is not linked to the incoming message anymore. This strategy
is the default strategy when manipulating streams of payloads.

Refer to the [signature list](signatures.md) to determine
which strategies are available for a specific method signature and
what’s the default strategy.

## Negative acknowledgement

Messages can also be *nacked*, which indicates that the message was not
processed correctly. The `Message.nack` method indicates failing
processing (and supply the reason), and, as for successful
acknowledgment, the *nack* is propagated through the chain of messages.

If the message has been produced by a connector, this connector
implements specific behavior when receiving a *nack*. It can fail
(default), or ignore the failing, or implement a dead-letter queue
mechanism. Refer to the connector documentation for further details
about the available strategies.

If the message is sent by an emitter using the `send(P)` method, the
returned `CompletionStage` is completed *exceptionally* with the *nack*
reason.

``` java
{{ insert('ack/NackOnEmitter.java', 'emitter') }}
```

Negative acknowledgment can be manual or automatic. If your method
handles instances of `Message` and the acknowledgment strategy is
`MANUAL`, you can nack a message explicitly. You must indicate the
reason (an exception) when calling the `nack` method. As for successful
acknowledgment, the `nack` returns a `CompletionStage` completed when
the `nack` has been processed.

If your method uses the `POST_PROCESSING` acknowledgment strategy, and
the method fails (either by throwing an exception or by producing a
failure), the message is automatically nacked with the caught exception:

``` java
{{ insert('ack/AutoNack.java', 'auto-nack') }}
```
