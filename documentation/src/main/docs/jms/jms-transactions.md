# JMS Transactions

The JMS connector supports transactional message consumption using the `transaction-mode` channel configuration property.
Three modes are available:

- `none` (default) — no transaction support, messages are acknowledged individually.
- `local` — JMS local (session) transactions. The session is committed on successful processing (ack) and rolled back on failure (nack).
- `xa` — XA distributed transactions. An XA transaction is started before each message receive, allowing other XA resources (e.g., databases) to participate in the same global transaction.

## Local Transactions

Local transactions use `SESSION_TRANSACTED` sessions.
Messages are processed sequentially — the next message is not received until the current one is committed or rolled back.
On ack, the session is committed. On nack, the session is rolled back and the message is redelivered by the broker.

### Configuration

```properties
mp.messaging.incoming.orders.connector=smallrye-jms
mp.messaging.incoming.orders.transaction-mode=local
```

!!!note
    The `session-mode` configuration is ignored when `transaction-mode=local`. The connector automatically uses `SESSION_TRANSACTED`.

### Example

```java
@ApplicationScoped
public class OrderProcessor {

    @Incoming("orders")
    public CompletionStage<Void> process(IncomingJmsMessage<?> message) {
        // Process the message...
        // On ack, the JMS session is committed.
        // On nack, the JMS session is rolled back and the message is redelivered.
        return message.ack();
    }
}
```

### Session Sharing with Outgoing Channels

When `transaction-mode` is `local` or `xa`, messages carry a `JmsSessionContext` metadata.
If the outgoing sink detects this metadata, it sends using the incoming transactional session context,
ensuring both the receive and send participate in the same JMS transaction.

## XA Distributed Transactions

XA mode enables distributed transactions spanning JMS and other XA-capable resources such as databases.
An XA transaction is started before each message is received, and the JMS XA resource is enlisted in the transaction.

### Prerequisites

XA transactions require:

- A `TransactionManager` (e.g., `quarkus-narayana-jta`)
- A **pooled** `XAConnectionFactory` (e.g., `quarkus-pooled-jms` or a JCA resource adapter such as `quarkus-ironjacamar`)

!!!warning
    Using a non-pooled `XAConnectionFactory` will cause severe performance degradation, as a new connection is created for every message poll.

### Configuration

```properties
mp.messaging.incoming.orders.connector=smallrye-jms
mp.messaging.incoming.orders.transaction-mode=xa
```

### Participating in the XA Transaction

The XA transaction is **suspended** after receive so it can be resumed on the processing thread.
Container integrations such as Quarkus typically provide a custom invoker that handles resume/suspend automatically.

Without a container invoker, the application must manage the lifecycle manually
using the `JmsXaTransactionMetadata` available on the message metadata:

```java
@ApplicationScoped
public class OrderProcessor {

    @Inject
    EntityManager entityManager;

    @Incoming("orders")
    public CompletionStage<Void> process(IncomingJmsMessage<?> message) {
        JmsXaTransactionMetadata xa = message.getMetadata(JmsXaTransactionMetadata.class)
                .orElseThrow();
        xa.resume();
        try {
            // Database and JMS participate in the same XA transaction
            entityManager.persist(toEntity(message.getPayload()));
        } finally {
            xa.suspend();
        }
        // On ack, the XA transaction is committed
        return message.ack();
    }
}
```

## Receive Timeout

The `receive-timeout` property controls how long (in milliseconds) the JMS consumer `receive` call blocks before returning `null`.
The default is `1000` ms.
Setting it to `0` blocks indefinitely until a message arrives.

```properties
mp.messaging.incoming.orders.receive-timeout=2000
```

## Custom Message Pollers

The `message-poller` property allows overriding the built-in polling behavior with a custom `JmsMessagePoller.Factory` CDI bean
qualified with `@Identifier`.

```java
@ApplicationScoped
@Identifier("my-poller")
public class MyPollerFactory implements JmsMessagePoller.Factory {

    @Override
    public JmsMessagePoller create(JmsConnectorIncomingConfiguration config,
            JmsResourceHolder<JMSConsumer> resourceHolder) {
        // resourceHolder is pre-configured and may be null for XA mode
        return () -> {
            jakarta.jms.Message received = resourceHolder.getClient().receive(500);
            return received != null ? Message.of(received) : null;
        };
    }
}
```

```properties
mp.messaging.incoming.orders.message-poller=my-poller
```

The commit and failure handlers remain determined by the `transaction-mode` — the custom poller only controls how messages are received.

## Failure Handling

Transaction modes work with all failure strategies (`fail`, `ignore`, `dead-letter-queue`).

- For `local` mode, nack triggers a session rollback before the failure strategy is applied.
- For `xa` mode, nack triggers an XA transaction rollback before the failure strategy is applied.

In both cases the message may be redelivered by the broker, depending on the broker configuration and the failure strategy used.
