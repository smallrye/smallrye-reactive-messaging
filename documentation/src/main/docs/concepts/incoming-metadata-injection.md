# Incoming Metadata Injection

!!!warning "Experimental"
    Metadata injection is an experimental feature.

When using reactive messaging, `Message` flow in your system.
Each message has a payload but can also contain _metadata_, as explained in [Messages, Payload, Metadata](concepts.md#messages-payload-metadata).

You can inject the incoming message metadata as a parameter depending on the method signature.
**Only methods receiving the payload as a parameter can also receive the metadata.**
For example, `@Incoming("in") void consume(String s)` can also receive the metadata using the following signature:
`@Incoming("in") void consume(String s, MyMetadata m)`.
Note that the payload must be the first parameter.

The metadata can be injected using the class of the metadata to inject.
In this case, the method is invoked with `null` if the metadata is missing.
You can also inject the metadata using `Optional<MetadataClass>,` which would be empty if the incoming message does not contain metadata of type `MetadataClass.`

``` java
{{ insert('incomings/MetadataInjectionExample.java', 'code') }}
```
