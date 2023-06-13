# Pulsar Transactions and Exactly-Once Processing

[Pulsar transactions](https://pulsar.apache.org/docs/3.0.x/txn-why/) enable event streaming applications to consume, process, and produce messages in one atomic operation.

Transactions allow one or multiple producers to send batch of messages to multiple topics where all messages in the batch are eventually visible to any consumer, or none is ever visible to consumers.

In order to be used, transaction support needs to be activated on the broker configuration:
```properties
# used to enable transaction coordinator
transactionCoordinatorEnabled=true

# used to create systemTopic used for transaction buffer snapshot
systemTopicEnabled=true
```

On the client side, the transaction support also needs to be enabled on `PulsarClient` configuration:

```properties
mp.messaging.outgoing.tx-producer.enableTransaction=true
```

Pulsar connector provides `PulsarTransactions` custom emitter for writing records inside a transaction.

It can be used as a regular emitter `@Channel`:

``` java
{{ insert('pulsar/outbound/PulsarTransactionalProducer.java') }}
```

The function given to the `withTransaction` method receives a `TransactionalEmitter` for producing records, and returns a `Uni` that provides the result of the transaction.
If the processing completes successfully, the producer is flushed and the transaction is committed.
If the processing throws an exception, returns a failing `Uni`, or marks the `TransactionalEmitter` for abort, the transaction is aborted.

!!!note
    Multiple transactional producers can participate in a single transaction.
    This ensures all messages are sent using the started transaction and before the transaction is committed, all participating producers are flushed.

If this method is called on a Vert.x context, the processing function is also called on that context.
Otherwise, it is called on the sending thread of the producer.

## Exactly-Once Processing

Pulsar Transactions API also allows managing consumer offsets inside a transaction, together with produced messages.
This in turn enables coupling a consumer with a transactional producer in a consume-transform-produce pattern,
also known as exactly-once processing.
It means that an application consumes messages, processes them, publishes the results to a topic, and commits offsets of the consumed messages in a transaction.

The `PulsarTransactions` emitter also provides a way to apply exactly-once processing to an incoming Pulsar message inside a transaction.

The following example includes a batch of Pulsar messages inside a transaction.

```properties
    mp.messaging.outgoing.tx-out-example.enableTransaction=true
    # ...
    mp.messaging.incoming.in-channel.enableTransaction=true
    mp.messaging.incoming.in-channel.batchReceive=true
```

``` java
{{ insert('pulsar/outbound/PulsarExactlyOnceProcessor.java') }}
```

If the processing completes successfully, the message is acknowledged inside the transaction and the transaction is committed.

!!!important
    When using exactly-once processing, messages can only be acked individually rather than cumulatively.

If the processing needs to abort, the message is nack'ed. One of the failure strategies can be employed in order to retry the processing or simply fail-stop.
Note that the `Uni` returned from the `#withTransaction` will yield a failure if the transaction fails and is aborted.

The application can choose to handle the error case, but for the message consumption to continue, `Uni` returned from the `@Incoming` method must not result in failure.
`PulsarTransactions#withTransactionAndAck` method will ack and nack the message but will not stop the reactive stream.
Ignoring the failure simply resets the consumer to the last committed offsets and resumes the processing from there.
