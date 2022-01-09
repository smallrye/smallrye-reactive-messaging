# Broadcast

!!!warning "Experimental"
    `@Broadcast` is an experimental feature.

By default, messages transiting in a channel are only dispatched to a
single consumer. Having multiple consumers is considered as an error,
and is reported at deployment time.

The {{ javadoc('io.smallrye.reactive.messaging.annotations.Broadcast') }}
annotation changes this behavior and indicates that messages transiting
in the channel are dispatched to all the consumers. `@Broadcast` must be
used with the `@Outgoing` annotation:

``` java
{{ insert('broadcast/BroadcastExamples.java', 'chain') }}
```

In the previous example, both consumers get the messages.

You can also control the number of consumers to wait before starting to
dispatch the messages. This allows waiting for the complete graph to be
woven:

``` java
{{ insert('broadcast/BroadcastWithCountExamples.java', 'chain') }}
```

!!!note
    Inbound connectors also support a `broadcast` attribute that allows
    broadcasting the messages to multiple downstream subscribers.


# Use with Emitter

For details on how to use `@Broadcast` with `Emitter` see the
[documentation](emitter.md#emitter-and-broadcast).
