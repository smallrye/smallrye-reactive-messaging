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
```

## Controlling the flow of messages

If a channel is configured to be pausable,
you can get the `PausableChannel` by channel name from the `ChannelRegistry` programmatically,
and pause or resume the channel as needed:

``` java
{{ insert('pausable/PausableController.java') }}
```

!!!warning
    Pausable channels only work with back-pressure aware subscribers, with bounded downstream requests.
