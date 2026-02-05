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
```

### Configuration Options

Pausable channels support the following configuration options:

- `pausable.initially-paused` - Whether the channel starts in a paused state (default: `false`)
- `pausable.late-subscription` - Whether to subscribe to the upstream after pausing (default: `false`)
- `pausable.buffer-size` - Maximum buffer size for items received while the channel is paused (optional)
- `pausable.buffer-strategy` - Strategy for handling already requested items that arrive while paused. Possible values are:
    * `BUFFER` - Buffer items received while paused and deliver them when resumed (default)
    * `DROP` - Drop already requested items received while paused
    * `IGNORE` - Let already requested items flow through even while paused

Example configuration:

```properties
mp.messaging.incoming.my-channel.pausable=true
mp.messaging.incoming.my-channel.pausable.initially-paused=true
mp.messaging.incoming.my-channel.pausable.buffer-size=100
mp.messaging.incoming.my-channel.pausable.buffer-strategy=BUFFER
mp.messaging.incoming.my-channel.pausable.late-subscription=false
```

## Controlling the flow of messages

If a channel is configured to be pausable,
you can inject the `PausableChannel` qualified with the `@Channel("channel-name")` annotation to control the flow of messages.
You can also get the `PausableChannel` by channel name from the `ChannelRegistry` programmatically,
and pause or resume the channel as needed:

``` java
{{ insert('pausable/PausableController.java') }}
```

!!!warning
    Pausable channels only work with back-pressure aware subscribers, with bounded downstream requests.
