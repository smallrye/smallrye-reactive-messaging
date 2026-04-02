# RabbitMQ Request/Reply

!!!warning "Experimental"
    RabbitMQ Request Reply Emitter is an experimental feature.

The RabbitMQ [Request-Reply](https://www.enterpriseintegrationpatterns.com/patterns/messaging/RequestReply.html) pattern allows you to publish a message to a RabbitMQ address and then await for a reply message that responds to the initial request, which can be used to implement [RPC over RabbitMQ](https://www.rabbitmq.com/tutorials/tutorial-six-java).

Given the following request/reply outgoing configuration:

```properties
mp.messaging.outgoing.my-request.connector=smallrye-rabbitmq
mp.messaging.outgoing.my-request.exchange.name=rpc
mp.messaging.outgoing.my-request.exchange.type=direct
mp.messaging.outgoing.my-request.default-routing-key=rpc-request
```

The `RabbitMQRequestReply` emitter implements the requestor (or the client) of the request-reply pattern for RabbitMQ outbound channels:

``` java
{{ insert('rabbitmq/outbound/RabbitMQRequestReplyEmitter.java') }}
```

The `request` method publishes the request message to the configured target address of the outgoing channel leveraging RabbitMQ [Direct Reply-To](https://www.rabbitmq.com/docs/direct-reply-to),
and waits for a reply.
When the reply is received the returned `Uni` is completed with the message payload.

The request send operation generates a correlation id and sets the `correlationId` property,
which it expects to be sent back in the reply message.

The replier (or the server) can be implemented using a Reactive Messaging processor:

```properties
mp.messaging.incoming.request.connector=smallrye-rabbitmq
mp.messaging.incoming.request.exchange.name=rpc
mp.messaging.incoming.request.exchange.type=direct
mp.messaging.incoming.request.routing-keys=rpc-request

mp.messaging.outgoing.reply.connector=smallrye-rabbitmq
mp.messaging.outgoing.reply.exchange.name=""
```

``` java
{{ insert('rabbitmq/outbound/RabbitMQReplier.java') }}
```

## Requesting with `Message` types

Like the core Emitter's `send` methods, `request` method also can receive a `Message` type and return a message:

``` java
{{ insert('rabbitmq/outbound/RabbitMQRequestReplyMessageEmitter.java') }}
```

!!! note
    The ingested reply type of the `RabbitMQRequestReply` is discovered at runtime,
    in order to configure a `MessageConverter` to be applied on the incoming message before returning the `Uni` result.

## Requesting multiple replies

You can use the `requestMulti` method to expect any number of replies represented by the `Multi` return type.

For example this can be used to aggregate multiple replies to a single request.

``` java
{{ insert('rabbitmq/outbound/RabbitMQRequestReplyMultiEmitter.java') }}
```
Like the other `request` you can also request `Message` types.

!!! note
    The channel attribute `reply.timeout` will be applied between each message, if reached the returned `Multi` will
    fail.

## Scaling Request/Reply

If multiple requestor instances are configured on the same outgoing address, and the same reply address,
each requestor instance will receive replies of all instances. If an observed correlation id doesn't match
the id of any pending replies, the reply is simply discarded.
With the additional network traffic this allows scaling requestors, (and repliers) dynamically.

## Pending replies and reply timeout

By default, the `Uni` returned from the `request` method is configured to fail with timeout exception if no replies is received after 5 seconds.
This timeout is configurable with the channel attribute `reply.timeout`.

A snapshot of the list of pending replies is available through the `RabbitMQRequestReply#getPendingReplies` method.

## Correlation Ids

The RabbitMQ Request/Reply allows configuring the correlation id mechanism completely through a `CorrelationIdHandler` implementation.
The default handler is based on randomly generated UUID strings.
The correlation id handler implementation can be configured using the `reply.correlation-id.handler` attribute.
As mentioned the default configuration is `uuid`.

Custom handlers can be implemented by proposing a CDI-managed bean with `@Identifier` qualifier.

## Reply Error Handling

If the reply server produces an error and can or would like to propagate the error back to the requestor, failing the returned `Uni`.

If configured using the `reply.failure.handler` channel attribute,
the `ReplyFailureHandler` implementations are discovered through CDI, matching the `@Identifier` qualifier.

A sample reply error handler can lookup header values and return the error to be thrown by the reply:

``` java
{{ insert('rabbitmq/outbound/MyRabbitMQReplyFailureHandler.java') }}
```

`null` return value indicates that no error has been found in the reply, and it can be delivered to the application.
