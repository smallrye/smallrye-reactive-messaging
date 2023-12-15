# Testing your application

It’s not rare to have to test your application but deploying the
infrastructure can be cumbersome. While Docker or Test Containers have
improved the testing experience, you may want to *mock* this
infrastructure.

SmallRye Reactive Messaging proposes an *in-memory* connector for this
exact purpose. It allows switching the connector used for a channel with
an *in-memory* connector. This in-memory connector provides a way to
send messages to incoming channels, or check the received messages for
outgoing channels.

To use the *in-memory* connector, you need to add the following
dependency to your project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-in-memory</artifactId>
  <version>{{ attributes['project-version'] }}</version>
  <scope>test</scope>
</dependency>
```

Then, in a test, you can do something like:

``` java
{{ insert('testing/MyTest.java') }}
```

When switching a channel to the in-memory connector, all the
configuration properties are ignored.

!!! warning
    This connector has been designed for testing purpose only.
    Switching the channel to in-memory connector means that the
    original connector is not invoked at all during tests.
    Therefore, if your code depends on a specific connector behaviour
    or a custom metadata you need to simulate those in your tests.


The *switch* methods return `Map<String, String>` instances containing
the set properties. While these system properties are already set, you
can retrieve them and pass them around, for example if you need to start
an external process with these properties:

``` java
{{ insert('testing/MyTestSetup.java', 'code') }}
```

!!!note
    The in-memory connector support the `broadcast` and `merge` attributes.
    So, if your connector is configured with `broadcast: true`, the
    connector broadcasts the messages to all the channel consumers. If your
    connector is configured with `merge:true`, the connector receives all
    the messages sent to the mapped channel even when coming from multiple
    producers.

## Vert.x Context with In-memory Connector

For the sake of simplicity, In-memory connector channels dispatch messages on the caller thread of `InMemorySource#send` method.
However, most of the other connectors handle context propagation dispatching messages on separate [duplicated Vert.x contexts](message-context.md).

If this causes a change of behaviour in your tests,
you can configure the in-memory connector channels with `run-on-vertx-context` attribute to dispatch events,
including messages and acknowledgements, on a Vert.x context.
Alternatively you can switch this behaviour using the `InMemorySource#runOnVertxContext` method.
