Cloud Events
============

This project illustrates the use of Cloud Event with MicroProfile Reactive Messaging.

## Messaging broker

This example makes no use of an external broker. Reactive messaging is done in memory.

## Start the application

The application can be started using:

```bash
mvn package exec:java
```

Then, looking at the output you can see successful incoming en outgoing messages.

## Anatomy

In addition to the commandline output, the application is composed of a single component:

* `MyCloudEventProcessor` - a bean consuming timer messages and sending a changing hello message.
* `MyCloudEventSink` - on the consuming side, the `Sink` retrieves messages and writes the message content to `stdout`.
* `MyCloudEventSource`  - a bean sending a timer message every second.

