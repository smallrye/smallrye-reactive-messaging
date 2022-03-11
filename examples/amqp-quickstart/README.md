AMQP Quickstart
================

This project illustrates how you can interact with AMQP using MicroProfile Reactive Messaging.

## AMQP broker

First you need a AMQP server. You can follow the instructions from the [ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) or run `docker-compose up` if you have docker installed on your machine.

## Start the application

The application can be started using:

```bash
mvn compile exec:java
```

Then, looking at the output you can see messages successfully send to and retrieved from a AMQP topic.

## Anatomy

In addition to the commandline output, the application is composed by 3 components:

* `BeanUsingAnEmitter` - a bean sending a changing hello message to AMQP topic every second.
* `Sender` - a bean sending a fixed message to a dynamic AMQP topic every 5 seconds.
* `Receiver`  - on the consuming side, the `Receiver` retrieves messages from a AMQP topic and writes the message content to `stdout`.

The interaction with AMQP is managed by MicroProfile Reactive Messaging.
The configuration is located in the microprofile config properties.
