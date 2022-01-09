# Advanced configuration

## Strict Binding Mode

By default, SmallRye Reactive Messaging does not enforce whether all
*mediators* are connected. It just prints a warning message. The strict
mode fails the deployment if some "incomings" are not bound to
"outgoings". To enable this mode, you can pass the
`-Dsmallrye-messaging-strict-binding=true` via the command line, or you
can set the `smallrye-messaging-strict-binding` attribute to `true` in
the configuration:

``` text
smallrye-messaging-strict-binding=true
```


## Disabling channels

You can disable a channel in the configuration by setting the `enabled`
attribute to `false`:

``` text
mp.messaging.outgoing.dummy-sink.connector=dummy
mp.messaging.outgoing.dummy-sink.enabled=false # Disable this channel
```

SmallRye Reactive Messaging does not register disabled channels, so make
sure the rest of the application does not rely on them.

## Publisher metrics

SmallRye Reactive Messaging integrates MicroProfile Metrics and
Micrometer for registering counter metrics (named
`mp.messaging.message.count`) of published messages per channel.

Both MicroProfile and Micrometer publisher metrics are enabled by
default if found on the classpath. They can be disabled with
`smallrye.messaging.metrics.mp.enabled` and
`smallrye.messaging.metrics.micrometer.enabled` properties respectively.

