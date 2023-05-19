# Using `KeyedMulti`

!!!warning "Experimental"
    `KeyedMulti` is an experimental feature.

When implementing a data streaming application, it's common to handle messages partitioned using a _key_.
In this case, your stream manipulation often has to 1) group by key and 2) do the manipulation.
Reactive Messaging can do the first step for you and reduce the code complexity.
To do this, it injects `io.smallrye.reactive.messaging.keyed.KeyedMulti` in your method instead of a _bare_ `Multi`.

For example, imagine the following stream, represented as `key:value`: `"a:1", "b:1", "a:2", "b:2", "b:3"...`
Then, let's consider the following method:

```java
{{ insert('keyed/KeyedMultiExample.java', 'code') }}
```

Reactive Messaging automatically extracts the key and value from the incoming stream and invokes the method for each key.
The received `KeyedMulti` represent the stream for each key.
The `key()` method returns the extracted key.

The key and value can be extracted from the payload but also (and often) from the message's metadata.

When using Kafka, it automatically extracts the key/value from the Kafka records.
In the other cases, or if you need custom extraction, you can implement your own `io.smallrye.reactive.messaging.keyed.KeyValueExtractor`.
Implementations are exposed `ApplicationScoped` beans, and are used to extract the key and value.
The following implementation extracts the key and value from payloads structured as "key:value":

```java
{{ insert('keyed/KeyValueExtractorFromPayload.java', 'code') }}
```

The extractor selection uses the `canExtract` method.
When multiple extractors are available, you can implement the `getPriority()` method to give a lower priority.
Default extractors have the priority 100.
So, if you have a custom extractor with the priority 99, it will be used (if it replies `true` to the `canExtract` call).
In addition, you can use the `io.smallrye.reactive.messaging.keyed.Keyed` annotation to indicate the class of the extractor to use.
The extractor must still be a CDI bean, but the `canExtract` method is not called, and priority does not matter:

```java
{{ insert('keyed/KeyedExample.java', 'code') }}
```
