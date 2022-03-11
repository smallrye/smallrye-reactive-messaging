Kafka Quickstart Kotlin
=======================

This project illustrates how you can interact with Apache Kafka using MicroProfile Reactive Messaging with an application written in Kotlin.

## Kafka cluster

First you need a Kafka cluster. You can follow the instructions from the [Apache Kafka web site](https://kafka.apache.org/quickstart) or run `docker-compose up` if you have docker installed on your machine.

## Start the application

The application can be started using:

```bash
mvn compile exec:java
```

Then, looking at the output you can see messages successfully send to and read from a Kafka topic.

## Anatomy

In addition to the commandline output, the application is composed by 3 components:

* `BeanUsingAnEmitter` - a bean sending a changing hello message to kafka topic every second.
* `Sender` - a bean sending a fixed message to a kafka topic every 5 seconds.
* `Receiver`  - on the consuming side, the `Receiver` retrieves messages from a kafka topic and writes the message content to `stdout`.

The interaction with Kafka is managed by MicroProfile Reactive Messaging.
The configuration is located in the microprofile config properties.
