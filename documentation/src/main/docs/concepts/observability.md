# Observability API

!!!important
    Observability API is experimental and SmallRye only feature.

Smallrye Reactive Messaging proposes an observability API that allows to observe messages received and send through inbound and outbound channels.

For any observation to happen, you need to provide an implementation of the `MessageObservationCollector`, discovered as a CDI-managed bean.

At wiring time the discovered `MessageObservationCollector` implementation `initObservation` method is called once per channel to initialize the `ObservationContext`.
The default `initObservation` implementation returns a default `ObservationContext` object,
but the collector implementation can provide a custom per-channel `ObservationContext` object that'll hold information necessary for the observation.
The `ObservationContext#complete` method is called each time a message observation is completed – message being acked or nacked.
The collector implementation can decide at initialization time to disable the observation per channel by returning a `null` observation context.

For each new message, the collector is on `onNewMessage` method with the channel name, the `Message` and the `ObservationContext` object initialized beforehand.
This method can react to the creation of a new message but also is responsible for instantiating and returning a `MessageObservation`.
While custom implementations can augment the observability capability, SmallRye Reactive Messaging provides a default implementation `DefaultMessageObservation`.

So a simple observability collector can be implemented as such:

``` java
{{ insert('observability/SimpleMessageObservationCollector.java', ) }}
```

A collector with a custom `ObservationContext` can be implemented as such :

``` java
{{ insert('observability/ContextMessageObservationCollector.java', ) }}
```
