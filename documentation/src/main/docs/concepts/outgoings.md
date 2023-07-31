# Multiple Outgoing Channels

!!!warning "Experimental"
    Multiple `@Outgoings` is an experimental feature.

The `@Outgoing` annotation is repeatable. It means that the method
dispatches outgoing messages to multiple listed channels:

``` java
{{ insert('outgoings/OutgoingsExample.java', 'code') }}
```

The default behaviour is same as the [@Broadcast annotation](broadcast.md),
meaning that outbound messages are dispatched to all listed outgoing channels.

However, different dispatching mechanism can be employed:

## Selectively dispatching messages using `Targeted` messages

You can selectively dispatch messages to multiple outgoings by returning
{{ javadoc('io.smallrye.reactive.messaging.Targeted') }} :

``` java
{{ insert('outgoings/TargetedExample.java', 'code') }}
```

In this example, three outgoing channels are declared on the `process` method
but in some condition channel `out3` does not receive any messages.

!!!important "Coordinated acknowledgements"
    `Targeted` return types coordinate acknowledgements between outgoing messages
    and the incoming message, therefore the incoming message will be ack'ed only
    when all outgoing messages are ack'ed.

In cases where you need to consume `Message` and handle metadata propagation
more finely you can use {{ javadoc('io.smallrye.reactive.messaging.TargetedMessages') }}
which is a `Message` type:

``` java
{{ insert('outgoings/TargetedExample.java', 'targeted-messages') }}
```

Note that in this case coordinated acknowledgements is handled explicitly
using {{ javadoc('io.smallrye.reactive.messaging.Messages') }} utility.

## Branching outgoing channels with `SplitterMulti`

In stream transformer processors it can be useful to branch out an incoming
stream into different sub-streams, based on some conditions.

When consuming `Multi`, returning a `SplitterMulti` allows to do that:

// TODO link to usage of `Multi.split` operation.

``` java
{{ insert('outgoings/SplitterMultiExample.java', 'code') }}
```

In this case the number of outgoing channels must match the number of branches given to `split` operation.

