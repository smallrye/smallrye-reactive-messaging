# Pausable Channels

Based on reactive streams, Smallrye Reactive Messaging ensures that channels are back-pressured.
This means that the flow of messages is controlled by the downstream consumer,
whether it is a processing method or outgoing channel.
Sometimes you may want to pause the flow of messages, for example, when the consumer is not ready to process them.

Injected [`@Channel`](emitter.md#retrieving-channels) streams are not subscribed to by default, so the flow of messages is controlled by the application.
But for [`@Incoming`](model.md#incoming-and-outgoing) methods, the flow of messages is controlled by the runtime.

Pausable channels are useful when you want to control the flow of messages within your application.

## Creating a Pausable Channel

To use pausable channels, you need to activate it with the configuration property `pausable` set to `true`.

```properties
mp.messaging.incoming.my-channel.pausable=true
# optional, by default the channel is NOT paused initially
mp.messaging.outgoing.my-channel.initially-paused=true
# optional, when enabled subscription to the upstream will be delayed until resume is called
mp.messaging.outgoing.my-channel.late-subscription=true
```

## Controlling the flow of messages

If a channel is configured to be pausable,
you can either inject it using the `@Channel("channel-name")` identifier, or retrieve it by channel name from the `ChannelRegistry` programmatically,
and pause or resume the channel as needed:

``` java
{{ insert('pausable/PausableController.java') }}
```

!!!warning
    Pausable channels only work with back-pressure aware subscribers, with bounded downstream requests.

## Working with concurrent consumers

Pausable channels work by blocking upstream requests when paused and letting them through when resumed.

This means, due to the asynchronous nature of reactive streams,
calling pause on a channel may not stop already requested messages from being dispatched to the consumer.
This is especially true when using concurrent consumers with `@Blocking(ordered = false)`.

If the upstream publisher (usually the consumer from an inbound source) produces already requested messages to a paused channel, pausable channel buffers those messages.
When the channel is resumed, the buffered messages are dispatched to the consumer.

This behavior is enabled by default and can be disabled by setting the configuration property `buffer-already-requested` to `false`:

```properties
mp.messaging.incoming.my-channel.pausable=true
mp.messaging.incoming.my-channel.buffer-already-requested=false
```
