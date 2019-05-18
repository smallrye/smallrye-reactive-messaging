MQTT Quickstart
================

This project illustrates how you can interact with MQTT using MicroProfile Reactive Messaging.

## MQTT broker

First you need a MQTT server. You can follow the instructions from the [Eclipse Mosquitto](https://mosquitto.org/) or run `docker-compose up` if you have docker installed on your machine.

## Start the application

The application can be started using: 

```bash
mvn package exec:java
```  

Then, looking at the output you can see messages successfully send to and retrieved from a MQTT topic.

## Anatomy

In addition to the commandline output, the application is composed by 3 components:

* `BeanUsingAnEmitter` - a bean sending a changing hello message to MQTT topic every second.
* `Sender` - a bean sending a fixed message to a dynamic MQTT topic every 5 seconds.
* `Receiver`  - on the consuming side, the `Receiver` retreives messages from a MQTT topic and writes the message content to `stdout`.

The interaction with MQTT is managed by MicroProfile Reactive Messaging.
The configuration is located in the microprofile config properties.
