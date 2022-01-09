# Message Converters

SmallRye Reactive Messaging supports *message converters*, allowing to
transform an incoming message into a version accepted by the method. If
the incoming messages or payload does not match the invoked method’s
expectation, SmallRye Reactive Messaging looks for a suitable converter.
If found, it converts the incoming message with this converter.

Converters can have multiple purposes, but the main use case is about
transforming the message’s payload:

``` java
{{ insert('converters/MyConverter.java', 'code') }}
```

To provide a converter, implement a bean exposing the `MessageConverter`
interface. The `canConvert` method is called during the lookup and
verifies if it can handle the conversion. The `target` type is the
expected payload type. If the converter returns `true` to `canConvert`,
SmallRye Reactive Messaging calls the `convert` method to proceed to the
conversion.

The previous converter can be used in application like the following, to
convert `Message<String>` to `Message<Person>`:

``` java
{{ insert('converters/ConverterExample.java', 'code') }}
```

Converters work for all supported method signatures. However, the
signature must be well-formed to allow the extraction of the expected
payload type. Wildcards and raw types do not support conversion. If the
expected payload type cannot be extracted, or no converter fits, the
message is passed as received.

If multiple suitable converters are present, implementations should
override the `getPriority` method returning the priority. The default
priority is `100`. The converter lookup invokes converters with higher
priority first.
