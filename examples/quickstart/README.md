Quickstart
==========

This project illustrates how MicroProfile Reactive Messaging works.

## Messaging broker

This example makes no use of an external broker. Reactive messaging is done in memory.

## Start the application

The application can be started using:

```bash
mvn compile exec:java
```

Then, looking at the output you can see successful incoming en outgoing messages.

## Logging

SmallRye Reactive Messaging uses JBoss Logging.
You can configure the output by configuring the `src/main/resources/logging.properties` and run the example with:

```bash
 mvn package exec:java -Dexec.mainClass=quickstart.Main -Djava.util.logging.config.file=./src/main/resources/logging.properties
```

In this case, JBoss Logging use the JDK Logger as backend.

## Anatomy

In addition to the commandline output, the application is composed of a single component:

* `Main` - main entry point as well a various (incoming and outgoing) messaging handlers.
