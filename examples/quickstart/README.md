Quickstart
==========

This project illustrates how MicroProfile Reactive Messaging works.

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

* `Main` - main entry point as well a various (incoming and outgoing) messaging handlers.
