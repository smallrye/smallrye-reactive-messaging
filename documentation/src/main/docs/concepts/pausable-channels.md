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

When `pausable` is not set, it follows the channel's [`graceful-shutdown`](graceful-shutdown.md) setting:
a channel with graceful shutdown enabled is pausable, as the shutdown pauses and drains it.
Graceful shutdown is enabled by default for every connector channel, incoming and outgoing, so they are pausable by default.
Set `pausable` explicitly to override it, for example `pausable=false` to opt out.

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

### Channels with concurrency

An incoming channel configured with `concurrency` is consumed through several indexed channels (`channel$1`, `channel$2`, ...).
The `PausableChannel` registered under the channel name controls all of them: pausing, resuming or draining it applies to every indexed channel.
Each indexed channel can still be controlled on its own, using its indexed name.

## Pause and Drain

The `PausableChannel` interface provides a `pauseAndDrain()` method that pauses the channel
and waits for all in-flight messages (delivered but not yet acked or nacked) to complete.

This is useful for operations that require no in-flight processing,
such as seeking a Kafka consumer to a new position or coordinating with external systems:

``` java
{{ insert('shutdown/PauseAndDrainExample.java', 'pause-drain') }}
```

The drain timeout is configurable via the `graceful-shutdown.drain-timeout` channel property (default: 10 seconds).

!!! note
    In-flight messages are only tracked when [`graceful-shutdown`](graceful-shutdown.md) is enabled for the channel.
    Without it, `pauseAndDrain()` pauses the channel and completes immediately, without waiting for in-flight messages.

`pauseAndDrain()` is also used internally during [graceful shutdown](graceful-shutdown.md)
to ensure that all in-flight messages are processed before closing connectors.
