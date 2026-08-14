# Testing your application

SmallRye Reactive Messaging provides a **test connector** that replaces
real broker connectors during tests. It lets you deliver messages to
incoming channels and verify messages sent to outgoing channels — without
requiring any external infrastructure.

Add the following dependency to your project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-in-memory</artifactId>
  <version>{{ attributes['project-version'] }}</version>
  <scope>test</scope>
</dependency>
```

Then, in a test, you can do something like:

``` java
{{ insert('testing/MyTest.java') }}
```

When switching a channel to the test connector, all the
configuration properties are ignored.

!!! warning
    This connector has been designed for testing purpose only.
    Switching the channel to the test connector means that the
    original connector is not invoked at all during tests.
    Therefore, if your code depends on a specific connector behaviour
    or a custom metadata you need to simulate those in your tests.


The *switch* methods return `Map<String, String>` instances containing
the set properties. While these system properties are already set, you
can retrieve them and pass them around, for example if you need to start
an external process with these properties:

``` java
{{ insert('testing/MyTestSetup.java', 'code') }}
```

!!!note
    The test connector supports the `broadcast` and `merge` attributes.
    So, if your connector is configured with `broadcast: true`, the
    connector broadcasts the messages to all the channel consumers. If your
    connector is configured with `merge:true`, the connector receives all
    the messages sent to the mapped channel even when coming from multiple
    producers.

## Testing with connector-specific emitters

Connector-specific emitter types such as `KafkaTransactions` or `KafkaRequestReply` work
with the test connector. When the channel uses the test connector instead of the native
connector, a fallback emitter is created automatically:

- **Transaction emitters** (`KafkaTransactions`, `PulsarTransactions`): Transaction
  semantics (begin/commit/abort) are skipped. Messages sent through the
  `TransactionalEmitter` are dispatched normally and can be verified via the `TestOutgoing`.
  The `markForAbort()` method is supported and causes the transaction `Uni` to fail with
  `TransactionAbortedException`. All `withTransaction` variants work, including
  exactly-once methods — the offset/acknowledgement management is skipped but the
  work function executes normally.

- **Request-reply emitters** (`KafkaRequestReply`, `AmqpRequestReply`,
  `RabbitMQRequestReply`): Requests are sent through the normal emitter path (visible
  in the `TestOutgoing`). Replies are resolved via a programmatic reply function that you
  configure in your test by casting the emitter to its no-op implementation and calling
  `setReplyFunction()`.

## Simulating connector-specific metadata

When your application reads connector-specific metadata from incoming messages,
you can attach it using the `deliver(payload, metadata...)` overload:

``` java
incoming.deliver(new Order(...),
    new IncomingKafkaRecordMetadata<>(
        new ConsumerRecord<>("orders", 0, 42L, "key", "value"), "orders"));
```

This creates a `Message` with the given payload and metadata, so your application
code can retrieve it via `message.getMetadata(IncomingKafkaRecordMetadata.class)`
or through metadata injection in `@Incoming` method parameters.

## Vert.x Context with Test Connector

For the sake of simplicity, test connector channels dispatch messages on the caller thread of `TestIncoming#deliver` method.
However, most of the other connectors handle context propagation dispatching messages on separate [duplicated Vert.x contexts](message-context.md).

If this causes a change of behaviour in your tests,
you can configure the test connector channels with `run-on-vertx-context` attribute to dispatch events,
including messages and acknowledgements, on a Vert.x context.
Alternatively you can switch this behaviour using the `TestIncoming#runOnVertxContext` method.

## Migrating from InMemoryConnector

The `InMemoryConnector` class and the `smallrye-in-memory` connector name are deprecated.
Use `TestingConnector` with the `smallrye-testing` connector name instead. The old API continues
to work — both connector names are supported simultaneously, and the old classes extend
the new ones.

| Deprecated | Replacement |
|---|---|
| `InMemoryConnector` | `TestingConnector` |
| `InMemorySource` | `TestIncoming` |
| `InMemorySink` | `TestOutgoing` |
| `connector.source("channel")` | `connector.incoming("channel")` |
| `connector.sink("channel")` | `connector.outgoing("channel")` |
| `source.send(payload)` | `incoming.deliver(payload)` |
| `sink.received()` | `outgoing.sent()` |
| `InMemoryConnector.switchIncomingChannelsToInMemory(...)` | `TestingConnector.switchIncomingChannelsToTesting(...)` |
| `InMemoryConnector.switchOutgoingChannelsToInMemory(...)` | `TestingConnector.switchOutgoingChannelsToTesting(...)` |
| `"smallrye-in-memory"` | `"smallrye-testing"` |
