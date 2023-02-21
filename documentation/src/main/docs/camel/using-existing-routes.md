# Using the Camel API

The Camel connector is based on the [Reactive Streams
support](https://camel.apache.org/components/latest/reactive-streams-component.html)
from Camel. If you have an application already using the Camel API
(routes, `from`...), you can integrate it with Reactive Messaging.

## Getting the CamelReactiveStreamsService

Once you add the Camel connector to your application, you can retrieve
the
`org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService`
object:

``` java
{{ insert('camel/api/CamelApi.java', 'reactive') }}
```

This `CamelReactiveStreamsService` lets you create `Publisher` and
`Subscriber` instances from existing routes.

## Using Camel Route with @Outgoing

If you have an existing Camel route, you can transform it as a
`Publisher` using the `CamelReactiveStreamsService`. Then, you can
return this `Publisher` from a method annotated with `@Outgoing`:

``` java
{{ insert('camel/api/CamelApi.java', 'source') }}
```

You can also use `RouteBuilder`:

``` java
{{ insert('camel/api/CamelApi.java', 'source-route-builder') }}
```

# Using Camel Route with @Incoming

If you have an existing Camel route, you can transform it as a
`Subscriber` using the `CamelReactiveStreamsService`. Then, you can
return this `Subscriber` from a method annotated with `@Incoming`:

``` java
{{ insert('camel/api/CamelApi.java', 'sink') }}
```

You can also use a producer:

``` java
{{ insert('camel/api/CamelApi.java', 'producer') }}
```
