# Generic Payloads

!!!warning "Experimental"
    Generic payloads are an experimental feature and the API is subject to change.

When using reactive messaging, `Message` flow in your system
each message has a payload but can also contain _metadata_, as explained in [Messages, Payload, Metadata](concepts.md#messages-payload-metadata).
The metadata can hold information, for example in an outgoing channel, additional properties of the outgoing message to be sent to the broker.

It is sometimes preferable to continue using the [payload signatures](model.md#messages-vs-payloads),
and also being able to attach metadata.
Using `GenericPayload` allows customizing metadata when handling payloads in SmallRye Reactive Messaging `@Incoming` and `@Outgoing` methods.
`GenericPayload` is a wrapper type, like the `Message`, containing a payload and metadata,
without requiring handling acknowledgments manually.

``` java
{{ insert('genericpayload/GenericPayloadExample.java', 'code') }}
```

You can combine generic payloads with [metadata injection](incoming-metadata-injection.md) :


``` java
{{ insert('genericpayload/GenericPayloadExample.java', 'injection') }}
```

Note that the metadata provided with the outgoing generic payload is merged with the incoming message metadata.

!!! warning "Limitations"
    There are several limitations for the use of `GenericPayload`:
    `GenericPayload` is not supported in emitters, as normal outgoing `Message` can be used for that purpose.
    While `GenericPayload<T>` can be used as an incoming payload type,
    [message converters](converters.md) are not applied to the payload type `T`.

