Google Cloud Pub/Sub Quickstart
================

This project illustrates how you can interact with Google Cloud Pub/Sub using MicroProfile Reactive Messaging.

## Start the application

The application can be started using:

```bash
mvn package exec:java
```

Then, looking at the output you can see messages successfully send to and retrieved from a Google Cloud Pub/Sub topic.

## Anatomy

In addition to the commandline output, the application is composed by 2 components:

* `BeanUsingAnEmitter` - a bean sending a changing test message to Google Cloud Pub/Sub topic every 10 second.
* `Receiver`  - on the consuming side, the `Receiver` retreives messages from a Google Cloud Pub/Sub topic and writes the message content to `stdout`.

The interaction with Google Cloud Pub/Sub is managed by MicroProfile Reactive Messaging.
The configuration is located in the microprofile config properties.
