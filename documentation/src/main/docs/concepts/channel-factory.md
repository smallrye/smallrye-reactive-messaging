# Channel Factory

The `ChannelFactory` API enables creating messaging channels at runtime, outside of the standard annotation-driven wiring.
This is useful when you need to create channels dynamically based on runtime conditions,
or when the channel configuration is not known at build time.

## Creating Incoming Channels

Inject `ChannelFactory` and call `incoming()` with the channel name and configuration:

``` java
{{ insert('channelfactory/ChannelFactoryIncomingExample.java', 'incoming') }}
```

The configuration must contain a `connector` attribute identifying the connector to use.
Additional properties are connector-specific (e.g., `topic`, `value.deserializer` for Kafka).

The returned `Flow.Publisher` has the full decorator chain applied (pausable channels, tracing, etc.)
and is registered in the `ChannelRegistry`.

## Creating Outgoing Channels

Create an outgoing channel with a programmatic `Emitter`:

``` java
{{ insert('channelfactory/ChannelFactoryOutgoingExample.java', 'outgoing') }}
```

The emitter type can be `Emitter`, `MutinyEmitter`, or any custom emitter type registered in the framework.

## Channel Binding

The `ChannelBinding` API provides a fluent way to subscribe to incoming channels with automatic payload conversion and ack/nack handling.
It combines channel creation with typed message consumption in a single API.

### Basic Subscription

``` java
{{ insert('channelfactory/ChannelBindingExample.java', 'subscribe') }}
```

On each message:

- The payload is converted to the target type using available `MessageConverter`s
- The consumer is invoked with the converted payload
- On success, the message is **acknowledged**
- On failure, the message is **negatively acknowledged** with the exception

### Subscription with Metadata

Access message metadata alongside the payload:

``` java
{{ insert('channelfactory/ChannelBindingExample.java', 'subscribe-metadata') }}
```

### Async Subscription

For asynchronous processing, return a `Uni<Void>` — the message is acknowledged when the `Uni` completes:

``` java
{{ insert('channelfactory/ChannelBindingExample.java', 'subscribe-async') }}
```

### Blocking Processing

Dispatch processing to worker threads, optionally with concurrency:

``` java
{{ insert('channelfactory/ChannelBindingExample.java', 'blocking') }}
```

### Processing Pipeline

Transform messages and forward them to an outgoing channel:

``` java
{{ insert('channelfactory/ChannelBindingExample.java', 'process') }}
```

The `process()` method transforms each incoming payload.
The `to()` terminal creates an outgoing channel and wires the processed stream to it.
The outgoing message preserves the incoming message's metadata and ack/nack chain.

## API Reference

### ChannelFactory

| Method | Description |
|---|---|
| `incoming(IncomingConnectorConfiguration, payloadType)` | Create an incoming channel binding with type conversion |
| `outgoing(OutgoingConnectorConfiguration, emitterType)` | Create an outgoing emitter |
| `outgoing(OutgoingConnectorConfiguration, source)` | Wire a source publisher to an outgoing connector |
| `outgoing(OutgoingConnectorConfiguration)` | Create an outgoing subscriber |
| `bind(publisher, payloadType)` | Create a channel binding from an existing publisher |
| `bind(publisher)` | Create a channel binding without type conversion |

Connector configuration objects are created using generated builders, e.g. `KafkaConnectorIncomingConfiguration.builder("channel").topic("my-topic").build()`.

### ChannelBinding

| Method | Description |
|---|---|
| `subscribe()` | Acknowledge and discard each message |
| `subscribe(Consumer)` | Process payload, auto-ack |
| `subscribe(BiConsumer)` | Process payload and metadata, auto-ack |
| `subscribe(Function<T, Uni<Void>>)` | Async processing, ack on completion |
| `blocking()` / `blocking(concurrency)` | Dispatch to worker threads |
| `blocking(executor)` / `blocking(executor, concurrency)` | Dispatch to custom executor |
| `process(Function)` / `processAsync(Function)` | Transform payloads |
| `to(subscriber)` / `to(OutgoingConnectorConfiguration)` | Forward to outgoing channel |
