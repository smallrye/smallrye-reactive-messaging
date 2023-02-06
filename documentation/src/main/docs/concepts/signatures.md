# Supported signatures

The following tables list the supported method signatures and indicate
the various supported features. For instance, they indicate the default
and available acknowledgement strategies (when applicable).

## Method signatures to generate data

| Signature                                           | Invocation time                                                                                                 |
|-----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `@Outgoing Publisher<Message<O>> method()` `        | Called once at *assembly* time                                                                                  |
| `@Outgoing Publisher<O> method()` `                 | Called once at *assembly* time                                                                                  |
| `@Outgoing Multi<Message<O>> method()` `            | Called once at *assembly* time                                                                                  |
| `@Outgoing Multi<O> method()` `                     | Called once at *assembly* time                                                                                  |
| `@Outgoing Flow.Publisher<Message<O>> method()` `   | Called once at *assembly* time                                                                                  |
| `@Outgoing Flow.Publisher<O> method()` `            | Called once at *assembly* time                                                                                  |
| `@Outgoing PublisherBuilder<Message<O>> method()` ` | Called once at *assembly* time                                                                                  |
| `@Outgoing PublisherBuilder<O> method()` `          | Called once at *assembly* time                                                                                  |
| `@Outgoing Message<O> method()` `                   | Called for every downstream request, sequentially                                                               |
| `@Outgoing O method()` `                            | Called for every downstream request, sequentially                                                               |
| `@Outgoing CompletionStage<Message<O>> method()` `  | Called for every downstream request, sequentially (After the completion of the last returned CompletionStage)   |
| `@Outgoing CompletionStage<O> method()` `           | Called for every downstream request, , sequentially (After the completion of the last returned CompletionStage) |
| `@Outgoing Uni<Message<O>> method()` `              | Called for every downstream request, sequentially (After the completion of the last returned Uni)               |
| `@Outgoing Uni<O> method()` `                       | Called for every downstream request, , sequentially (After the completion of the last returned Uni)             |

## Method signatures to consume data

| Signature                                              | Invocation time                                  | Supported Acknowledgement Strategies            |
|--------------------------------------------------------|--------------------------------------------------|-------------------------------------------------|
| `@Incoming void method(I p)`                           | Called for every incoming payload (sequentially) | *POST_PROCESSING*, NONE, PRE_PROCESSING         |
| `@Incoming CompletionStage<?> method(Message<I> msg)`  | Called for every incoming message (sequentially) | *MANUAL*, NONE, PRE_PROCESSING                  |
| `@Incoming CompletionStage<?> method(I p)`             | Called for every incoming payload (sequentially) | *POST_PROCESSING*, PRE_PROCESSING, NONE         |
| `@Incoming Uni<?> method(Message<I> msg)`              | Called for every incoming message (sequentially) | *MANUAL*, NONE, PRE_PROCESSING                  |
| `@Incoming Uni<?> method(I p)`                         | Called for every incoming payload (sequentially) | *POST_PROCESSING*, PRE_PROCESSING, NONE         |
| `@Incoming Subscriber<Message<I>> method()`            | Called once at *assembly* time                   | *MANUAL*, POST_PROCESSING, NONE, PRE_PROCESSING |
| `@Incoming Subscriber<I> method()`                     | Called once at *assembly* time                   | *POST_PROCESSING*, NONE, PRE_PROCESSING         |
| `@Incoming Flow.Subscriber<Message<I>> method()`       | Called once at *assembly* time                   | *MANUAL*, POST_PROCESSING, NONE, PRE_PROCESSING |
| `@Incoming Flow.Subscriber<I> method()`                | Called once at *assembly* time                   | *POST_PROCESSING*, NONE, PRE_PROCESSING         |
| `@Incoming SubscriberBuilder<Message<I>, ?> method()`  | Called once at *assembly* time                   | *MANUAL*, POST_PROCESSING, NONE, PRE_PROCESSING |
| `@Incoming SubscriberBuilder<I, ?> method()`           | Called once at *assembly* time                   | *MANUAL*, POST_PROCESSING, NONE, PRE_PROCESSING |

## Method signatures to process data

| Signature                                                                 | Invocation time                                  | Supported Acknowledgement Strategies    | Metadata Propagation |
|---------------------------------------------------------------------------|--------------------------------------------------|-----------------------------------------|----------------------|
| `@Outgoing @Incoming Message<O> method(Message<I> msg)`                   | Called for every incoming message (sequentially) | *MANUAL*, NONE, PRE_PROCESSING          | manual               |
| `@Outgoing @Incoming O method(I payload)`                                 | Called for every incoming payload (sequentially) | *POST_PROCESSING*, NONE, PRE_PROCESSING | automatic            |
| `@Outgoing @Incoming CompletionStage<Message<O>> method(Message<I> msg)`  | Called for every incoming message (sequentially) | *MANUAL*, NONE, PRE_PROCESSING          | manual               |
| `@Outgoing @Incoming CompletionStage<O> method(I payload)`                | Called for every incoming payload (sequentially) | *POST_PROCESSING*, NONE, PRE_PROCESSING | automatic            |
| `@Outgoing @Incoming Uni<Message<O>> method(Message<I> msg)`              | Called for every incoming message (sequentially) | *MANUAL*, NONE, PRE_PROCESSING          | manual               |
| `@Outgoing @Incoming Uni<O> method(I payload)`                            | Called for every incoming payload (sequentially) | *POST_PROCESSING*, NONE, PRE_PROCESSING | automatic            |
| `@Outgoing @Incoming Processor<Message<I>, Message<O>> method()`          | Called once at *assembly* time                   | *MANUAL*, PRE_PROCESSING, NONE          | manual               |
| `@Outgoing @Incoming Processor<I, O> method()`                            | Called once at *assembly* time                   | *PRE_PROCESSING*, NONE                  | not supported        |
| `@Outgoing @Incoming Flow.Processor<Message<I>, Message<O>> method()`     | Called once at *assembly* time                   | *MANUAL*, PRE_PROCESSING, NONE          | manual               |
| `@Outgoing @Incoming Flow.Processor<I, O> method()`                       | Called once at *assembly* time                   | *PRE_PROCESSING*, NONE                  | not supported        |
| `@Outgoing @Incoming ProcessorBuilder<Message<I>, Message<O>> method()`   | Called once at *assembly* time                   | *MANUAL*, PRE_PROCESSING, NONE          | manual               |
| `@Outgoing @Incoming ProcessorBuilder<I, O> method()`                     | Called once at *assembly* time                   | *PRE_PROCESSING*, NONE                  | not supported        |
| `@Outgoing @Incoming Publisher<Message<O>> method(Message<I> msg)`        | Called once at *assembly* time                   | *MANUAL*, PRE_PROCESSING, NONE          | manual               |
| `@Outgoing @Incoming Publisher<O> method(I payload)`                      | Called once at *assembly* time                   | *PRE_PROCESSING*, NONE                  | automatic            |
| `@Outgoing @Incoming Multi<Message<O>> method(Message<I> msg)`            | Called once at *assembly* time                   | *MANUAL*, PRE_PROCESSING, NONE          | manual               |
| `@Outgoing @Incoming Multi<O> method(I payload)`                          | Called once at *assembly* time                   | *PRE_PROCESSING*, NONE                  | automatic            |
| `@Outgoing @Incoming Flow.Publisher<Message<O>> method(Message<I> msg)`   | Called once at *assembly* time                   | *MANUAL*, PRE_PROCESSING, NONE          | manual               |
| `@Outgoing @Incoming Flow.Publisher<O> method(I payload)`                 | Called once at *assembly* time                   | *PRE_PROCESSING*, NONE                  | automatic            |
| `@Outgoing @Incoming PublisherBuilder<Message<O>> method(Message<I> msg)` | Called once at *assembly* time                   | *MANUAL*, PRE_PROCESSING, NONE          | manual               |
| `@Outgoing @Incoming PublisherBuilder<O> method(I payload)`               | Called once at *assembly* time                   | *PRE_PROCESSING*, NONE                  | automatic            |

## Method signatures to manipulate streams

| Signature                                                                                   | Invocation time                | Supported Acknowledgement Strategies | Metadata Propagation |
|---------------------------------------------------------------------------------------------|--------------------------------|--------------------------------------|----------------------|
| `@Outgoing @Incoming Publisher<Message<O>> method(Publisher<Message<I>> pub)`               | Called once at *assembly* time | *MANUAL*, NONE, PRE_PROCESSING       | manual               |
| `@Outgoing @Incoming Publisher<O> method(Publisher<I> pub)`                                 | Called once at *assembly* time | *PRE_PROCESSING*, NONE               | not supported        |
| `@Outgoing @Incoming Multi<Message<O>> method(Multi<Message<I>> pub)`                       | Called once at *assembly* time | *MANUAL*, NONE, PRE_PROCESSING       | manual               |
| `@Outgoing @Incoming Multi<O> method(Multi<I> pub)`                                         | Called once at *assembly* time | *PRE_PROCESSING*, NONE               | not supported        |
| `@Outgoing @Incoming Flow.Publisher<Message<O>> method(Flow.Publisher<Message<I>> pub)`     | Called once at *assembly* time | *MANUAL*, NONE, PRE_PROCESSING       | manual               |
| `@Outgoing @Incoming Flow.Publisher<O> method(Flow.Publisher<I> pub)`                       | Called once at *assembly* time | *PRE_PROCESSING*, NONE               | not supported        |
| `@Outgoing @Incoming PublisherBuilder<Message<O>> method(PublisherBuilder<Message<I>> pub)` | Called once at *assembly* time | *MANUAL*, NONE, PRE_PROCESSING       | manual               |
| `@Outgoing @Incoming PublisherBuilder<O> method(PublisherBuilder<I> pub)`                   | Called once at *assembly* time | NONE, PRE_PROCESSING                 | not supported        |


!!!important
    When processing `Message`, it is often required to *chain* the incoming
    `Message` to enable post-processing acknowledgement and metadata
    propagation. Use the `with` (like `withPayload`) methods from the
    incoming message, so it copies the metadata and ack/nack methods. It
    returns a new `Message` with the right content.

