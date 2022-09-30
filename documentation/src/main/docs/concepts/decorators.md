# Channel Decorators

SmallRye Reactive Messaging supports decorating reactive streams
of incoming and outgoing channels for implementing cross-cutting
concerns such as monitoring, tracing or message interception.

Two symmetrical APIs are proposed for decorating publisher and subscriber channels,
{{ javadoc('io.smallrye.reactive.messaging.PublisherDecorator') }}
and {{ javadoc('io.smallrye.reactive.messaging.SubscriberDecorator') }} respectively.

!!!Important
    `@Incoming` channels and channels bound to an outbound connector are both `Subscriber`s.
    Conversely `@Outgoing` channels and channels bound to an inbound connector are `Publisher`s.

For example, to provide a decorator which counts consumed messages from incoming connector,
implement a bean exposing the interface `PublisherDecorator`:

``` java
{{ insert('decorators/ConsumedMessageDecorator.java', 'code') }}
```

Decorators' `decorate` method is called only once per channel at application deployment when graph wiring is taking place.
Decorators are very powerful because they receive the stream of messages (Mutiny `Multi<Message<?>>`)
and potentially return a new stream of messages.

Note that if a decorator is available it will be called on every channel.
The `decorate` method receives the channel name and whether the channel is a connector or not as parameters.
Decorators are called ordered from highest to lowest priority (from the least value to the greatest),
obtained using the `jakarta.enterprise.inject.spi.Prioritized#getPriority` method.

!!!Note
    The `SubscriberDecorator` receive a list of channel names because `@Incoming` annotation is repeatable
    and consuming methods can be linked to multiple channels.

## Intercepting Outgoing Messages

Decorators can be used to intercept and alter messages, both on incoming and outgoing channels.
Smallrye Reactive Messaging provides a `SubscriberDecorator` by default to allow intercepting outgoing messages for a specific channel.

To provide an outgoing interceptor implement a bean exposing the interface {{ javadoc('io.smallrye.reactive.messaging.OutgoingInterceptor') }}, qualified with a `@Identifier` with the channel name to intercept.
Only one interceptor is allowed to be bound for interception per outgoing channel.
If no interceptors are found with a `@Identifier` but a `@Default` one is available, it is used.
When multiple interceptors are available, the bean with the highest priority is used.

``` java
{{ insert('interceptors/MyInterceptor.java') }}
```

An `OutgoingInterceptor` can implement these three methods:

- `Message<?> onMessage(Message<?> message)` : Called before passing the message to the outgoing connector for transmission.
The message can be altered by returning a new message from this method.
- `void onMessageAck(Message<?> message)` : Called after message acknowledgment.
This callback can access `OutgoingMessageMetadata` which will hold the result of the message transmission to the broker, if supported by the connector. This is only supported by MQTT and Kafka connectors.
- `void onMessageNack(Message<?> message, Throwable failure)` : Called before message negative-acknowledgment.

!!!Note
    If you are willing to adapt an incoming message payload to fit a consuming method receiving type,
    you can use `MessageConverter`s.
