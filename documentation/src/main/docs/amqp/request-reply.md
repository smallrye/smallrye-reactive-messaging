# AMQP Request/Reply

!!!warning "Experimental"
    AMQP Request Reply Emitter is an experimental feature.

The AMQP [Request-Reply](https://www.enterpriseintegrationpatterns.com/patterns/messaging/RequestReply.html) pattern allows you to publish a message to an AMQP address and then await for a reply message that responds to the initial request.

The `AmqpRequestReply` emitter implements the requestor (or the client) of the request-reply pattern for AMQP 1.0 outbound channels:

``` java
{{ insert('amqp/outbound/AmqpRequestReplyEmitter.java') }}
```

The `request` method publishes the request message to the configured target address of the outgoing channel,
and listens on a reply address (by default, the channel name with `-reply` suffix) for a reply message.
When the reply is received the returned `Uni` is completed with the message payload.

The request send operation generates a correlation id and sets the AMQP `message-id` property,
which it expects to be sent back in the reply message's `correlation-id` property.

The replier (or the server) can be implemented using a Reactive Messaging processor:

``` java
{{ insert('amqp/outbound/AmqpReplier.java') }}
```

When you need more control over the reply message, such as setting the reply-to address and correlation id explicitly, you can use the `Message` type:

``` java
{{ insert('amqp/outbound/AmqpReplierWithMetadata.java') }}
```

Given the following configuration example:

```properties
mp.messaging.outgoing.my-request.connector=smallrye-amqp
mp.messaging.outgoing.my-request.address=requests
mp.messaging.outgoing.my-request.reply.address=my-request-reply

mp.messaging.incoming.request.connector=smallrye-amqp
mp.messaging.incoming.request.address=requests

mp.messaging.outgoing.reply.connector=smallrye-amqp
mp.messaging.outgoing.reply.address=my-request-reply
mp.messaging.outgoing.reply.use-anonymous-sender=false
```

## Requesting with `Message` types

Like the core Emitter's `send` methods, `request` method also can receive a `Message` type and return a message:

``` java
{{ insert('amqp/outbound/AmqpRequestReplyMessageEmitter.java') }}
```

!!! note
    The ingested reply type of the `AmqpRequestReply` is discovered at runtime,
    in order to configure a `MessageConverter` to be applied on the incoming message before returning the `Uni` result.

## Requesting multiple replies

You can use the `requestMulti` method to expect any number of replies represented by the `Multi` return type.

For example this can be used to aggregate multiple replies to a single request.

``` java
{{ insert('amqp/outbound/AmqpRequestReplyMultiEmitter.java') }}
```
Like the other `request` you can also request `Message` types.

!!! note
    The channel attribute `reply.timeout` will be applied between each message, if reached the returned `Multi` will
    fail.

## Pending replies and reply timeout

By default, the `Uni` returned from the `request` method is configured to fail with timeout exception if no reply is received after 5 seconds.
This timeout is configurable with the channel attribute `reply.timeout` (in milliseconds).

```properties
mp.messaging.outgoing.my-request.reply.timeout=10000
```

A snapshot of the list of pending replies is available through the `AmqpRequestReply#getPendingReplies` method.

## Scaling Request/Reply

If multiple requestor instances are configured on the same outgoing address, and the same reply address,
each requestor instance will receive replies of all instances. If an observed correlation id doesn't match
the id of any pending replies, the reply is simply discarded.
With the additional network traffic this allows scaling requestors, (and repliers) dynamically.

## Correlation Ids

The AMQP Request/Reply allows configuring the correlation id mechanism completely through a `CorrelationIdHandler` implementation.
The default handler is based on randomly generated UUID strings, set as the AMQP `message-id` on the request message.
The reply message is expected to carry the same value in the `correlation-id` property.

The correlation id handler implementation can be configured using the `reply.correlation-id.handler` attribute.
The default configuration is `uuid`,
and an alternative `bytes` implementation can be used to generate random binary correlation ids.

Custom handlers can be implemented by proposing a CDI-managed bean with `@Identifier` qualifier.

## Reply Error Handling

If the reply server produces an error, it can propagate the error back to the requestor, failing the returned `Uni`.

If configured using the `reply.failure.handler` channel attribute,
the `ReplyFailureHandler` implementations are discovered through CDI, matching the `@Identifier` qualifier.

A sample reply error handler can lookup application properties and return the error to be thrown by the reply:

``` java
{{ insert('amqp/outbound/MyAmqpReplyFailureHandler.java') }}
```

`null` return value indicates that no error has been found in the reply message, and it can be delivered to the application.

## Using Link Pairing

AMQP 1.0 supports the concept of _link pairing_ where sender and receiver links are created as a paired unit on the same connection.
When link pairing is enabled, the sender link and the reply receiver link are paired at the broker level,
allowing the sender to reference the receiver's address as the reply-to target using the special `$me` address.

To enable link pairing:

```properties
mp.messaging.outgoing.my-request.connector=smallrye-amqp
mp.messaging.outgoing.my-request.address=requests
mp.messaging.outgoing.my-request.link-pairing=true
mp.messaging.outgoing.my-request.container-id=my-request-client
```

When `link-pairing` is enabled and no explicit `reply.address` is set, the reply address defaults to `$me`,
meaning the reply receiver's address is resolved by the broker through the paired link.

You can still provide an explicit reply address with link pairing:

```properties
mp.messaging.outgoing.my-request.link-pairing=true
mp.messaging.outgoing.my-request.reply.address=my-reply-queue
```

### Implementing a paired replier

The replier (server) can also use link pairing by configuring both the incoming and outgoing channels
with the same `container-id` and `link-name`, and setting `link-pairing=true`:

```properties
# Server incoming config
mp.messaging.incoming.server-in.connector=smallrye-amqp
mp.messaging.incoming.server-in.address=requests
mp.messaging.incoming.server-in.container-id=reply-server
mp.messaging.incoming.server-in.link-name=reply-server-link
mp.messaging.incoming.server-in.link-pairing=true

# Server outgoing config
mp.messaging.outgoing.server-out.connector=smallrye-amqp
mp.messaging.outgoing.server-out.address=$me
mp.messaging.outgoing.server-out.container-id=reply-server
mp.messaging.outgoing.server-out.link-name=reply-server-link
mp.messaging.outgoing.server-out.link-pairing=true
```

## Connection Sharing

Multiple AMQP channels can share the same underlying connection when configured with the same `container-id`.
This reduces resource consumption and is particularly useful for request-reply patterns
where the sender and reply receiver can share a single connection.

```properties
mp.messaging.outgoing.my-request.connector=smallrye-amqp
mp.messaging.outgoing.my-request.address=requests
mp.messaging.outgoing.my-request.container-id=my-connection
mp.messaging.outgoing.my-request.reply.address=replies

mp.messaging.incoming.other-channel.connector=smallrye-amqp
mp.messaging.incoming.other-channel.address=other
mp.messaging.incoming.other-channel.container-id=my-connection
```

Both channels above will share the same AMQP connection because they use the same `container-id`.

!!! note
    Connection sharing requires all channels with the same `container-id` to have compatible connection settings (host, port, credentials, etc.).
    If the settings differ, the connector will detect the conflict and raise an error.

When using link pairing with request-reply, the outgoing channel and its reply receiver automatically share a connection
when configured with the same `container-id`.

## Using with RabbitMQ

RabbitMQ 4.0+ with the native AMQP 1.0 support provides the `amq.rabbitmq.reply-to` pseudo-address for
[Direct Reply-To](https://www.rabbitmq.com/docs/direct-reply-to),
which dynamically creates volatile reply-to queues.

To use direct reply-to with request-reply:

```properties
mp.messaging.outgoing.my-request.connector=smallrye-amqp
mp.messaging.outgoing.my-request.address=/queues/requests
mp.messaging.outgoing.my-request.reply.address=amq.rabbitmq.reply-to
mp.messaging.outgoing.my-request.container-id=my-request
mp.messaging.outgoing.my-request.use-anonymous-sender=false
```

When using `amq.rabbitmq.reply-to`, the broker creates a dynamic volatile queue for replies.
The actual queue address is resolved at connection time and used as the `reply-to` property on request messages.

!!! note
    With RabbitMQ, remember to use `/queues/` prefixed addresses (v2 address format)
    and set `use-anonymous-sender=false` as anonymous senders are not supported.

Link pairing is also supported with RabbitMQ, see [Using Link Pairing](#using-link-pairing) above for configuration details.
