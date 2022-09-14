RabbitMQ Quickstart
================

This project illustrates how you can interact with RabbitMQ using MicroProfile Reactive Messaging.

## RabbitMQ broker

First you need a RabbitMQ broker. You can run `docker-compose up` if you have docker installed on your machine.

## Start the application

The application can be started using:

```bash
mvn compile exec:java
```

Then, looking at the output you can see messages successfully send to and retrieved.

```
INFO: SRMSG17033: A message sent to channel `from-rabbitmq-jsonobject` has been ack'd
received string: Price from emitter 0
INFO: SRMSG17033: A message sent to channel `from-rabbitmq-jsonobject` has been ack'd
received string: 0
INFO: SRMSG17033: A message sent to channel `from-rabbitmq-string` has been ack'd
received jsonobject price: 0
```

## Anatomy

In addition to the commandline output, the application is composed by 3 components:

* `BeanUsingAnEmitter` - a bean sending a changing message to RabbitMQevery second.
* `Sender` - a bean sending a changing message to RabbitMQevery second.
* `Receiver`  - on the consuming side, the `Receiver` retrieves messages from RabbitMQ and writes the message content to `stdout`.

The interaction with RabbitMQ is managed by MicroProfile Reactive Messaging.
The configuration is located in the microprofile config properties.
