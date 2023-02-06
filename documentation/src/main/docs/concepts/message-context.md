# Message Contexts

Message context provides a way to propagate data along the processing of a message.
It can be used to propagate message specific objects in an implicit manner and be able to retrieve them later, such as the user, session or transaction.

!!!important
    Message contexts are only support by Kafka, AMQP, RabbitMQ and MQTT connectors.

!!!note
    Message context support is an experimental and SmallRye only feature.

## What's a message context

A message context is execution context on which a message is processed.
Each stage of the processing is going to use the same execution context.
Thus, it allows storing _data_ which can later be restored.
For example, you can imagine storing some authentication (`User` in the following example) data in one part of your processing and restore it in a later stage.

```java
@Incoming("data")
@Outgoing("process")
public Message<String> process(Message<String> input) {
    // Extract some data from the message and store it in the context
    User user = ...;
    // Store the extracted data into the message context.
    ContextLocals.put("user", user);
    record input;
}

@Incoming("process")
@Outgoing("after-process")
public String handle(String payload) {
   // You can retrieve the store data using
   User user = ContextLocals.get("user", null);

   // ...
   return payload;
}
```
The Message context is also available when using blocking or asynchronous stages (stage returning `Uni` or `CompletionStage`)

## The difference with metadata

Message metadata can be used to provide a similar feature.
However, it requires using `Messages` which can be inconvenient (need to handle the acknowledgement manually).
Message Contexts provide a simpler API, closer to a _Message CDI scope_: you can save data, and restore it later.
The implicit propagation avoid having to deal with `Messages`.

## Supported signatures

Message context works with:

* methods consuming or producing `Messages`, `Uni<Message<T>>` and `CompletionStage<Message<T>>`
* methods consuming or producing payloads, `Uni<Payload>` and `CompletionStage<Payload>`.
* blocking and non-blocking methods

However, message context are **NOT** enforced when using methods consuming or producing:

* `Multi`, `Flow.Publisher`, `Publisher` and `PublisherBuilder`
* `Subscriber`, `Flow.Subscriber`, and `SubscriberBuilder`
* `Processor`, `Flow.Processor`, and `ProcessorBuilder`


## Under the hood

Under the hood, the message context feature uses Vert.x _duplicated contexts_.
A duplicated context is a view of the "root" (event loop) context, which is restored at each stage of the message processing.

Each time that a compatible connector receives a message from a broker, it creates a new _duplicated context_ and attaches it to the message.
So the context is stored in the metadata of the message.

When the message is processed, SmallRye Reactive Messaging makes sure that this processing is executed on the stored duplicated context.
