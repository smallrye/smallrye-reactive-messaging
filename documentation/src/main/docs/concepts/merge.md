# Merge channels

!!!warning "Experimental"
    `@Merge` is an experimental feature.


By default, messages transiting in a channel can arise from a single
producer. Having multiple producers is considered erroneous and is
reported at deployment time.

The {{ javadoc('io.smallrye.reactive.messaging.annotations.Merge') }}
annotation changes this behavior and indicates that a channel can have
multiple producers. `@Merge` must be used with the `@Incoming`
annotation:

``` java
{{ insert('merge/MergeExamples.java', 'chain') }}
```

In the previous example, the consumer gets all the messages (from both
producers).

The `@Merge` annotation allows configuring how the incoming messages
(from the different producers) are merged into the channel. The `mode`
attribute allows configuring this behavior:

-   `ONE` picks a single producer, discarding the other producer;

-   `MERGE` (default) gets all the messages as they come, without any
    defined order. Messages from different producers may be interleaved.

-   `CONCAT` concatenates the producers. The messages from one producer
    are received until the messages from other producers are received.

!!!note
    Outbound connectors also support a `merge` attribute that allows
    consuming the messages to multiple upstreams. It will dispatch all the
    received messages.

