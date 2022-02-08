# Kafka Companion

!!!warning "Experimental"
    Kafka Companion is experimental and APIs are subject to change in the future.

The **Kafka Companion** is a separate Java library for helping to test Kafka applications.
It is not intended to mock Kafka, but to the contrary, connect to a Kafka broker and provide high-level features.

It is not limited to the SmallRye Reactive Messaging testing, but intends to improve the testability of applications using Kafka. Some of its features:

* Running In-container Kafka broker for tests via [strimzi-test-container](https://github.com/strimzi/test-container).
* Running the Kafka broker behind a [toxiproxy](https://github.com/Shopify/toxiproxy) for simulating network issues.
* Running embedded Kafka Kraft broker for tests.
* Base classes for tests to bootstrap tests.
* Companion classes for easily creating tasks to produce and consume Kafka records.
* Writing assertions for produce and consume tasks, state of consumers, topics, offsets etc.

## Getting started writing tests

Easiest way of starting to write Kafka tests is to extend `KafkaCompanionTestBase`.
It starts a single-node Kafka broker for the test suite using testcontainers and creates the `KafkaCompanion` to connect to this broker:

``` java
{{ insert('kafka/companion/KafkaWithBaseTest.java', 'code') }}
```

`KafkaCompanion` can be used on its own to connect to a broker:

``` java
{{ insert('kafka/companion/KafkaTest.java', 'companion') }}
```


There are a couple of things to note on how Kafka companion handles **producers**, **consumers** and **tasks**:

`ProducerBuilder` and `ConsumerBuilder` lazy descriptions of with which configuration to create a producer or a consumer.

`ProducerTask` and `ConsumerTask` run asynchronously on dedicated threads and are started as soon as they are created.
A task terminates when it is explicitly stopped, when it's predefined duration or number of records has been produced/consumed, or when it encounters an error.
An exterior thread can await on records processed, or simply on termination of the task.
At a given time produced or consumed records are accessible through the task.

The actual creation of the producer or consumer happens when a task is started. When the task terminates the producer or the consumer is automatically closed.

For example, in the previous example:

1. We described a producer with a client id `my-producer` and `max.block.ms` of 5 seconds.
2. Then we created a task to produce 100 records using the generator function, without waiting for its completion.
3. We then described a consumer with group id `my-group` and which commits offset synchronously on every received record.
4. Finally, we created a task to consume records for 10 seconds and await its completion.

## Producing records

### Produce from records

Produce given records:
``` java
{{ insert('kafka/companion/ProducerTest.java', 'records') }}
```

### Produce from generator function

Produce 10 records using the generator function:
``` java
{{ insert('kafka/companion/ProducerTest.java', 'generator') }}
```

### Produce from CSV file

Given a comma-separated file `records.csv` with the following content
```csv
messages,0,a,asdf
messages,1,b,asdf
messages,3,c,asdf
```

Produce records from the file:
``` java
{{ insert('kafka/companion/ProducerTest.java', 'csv') }}
```

## Consuming records

### Consume from topics

``` java
{{ insert('kafka/companion/ConsumerTest.java', 'topics') }}
```

### Consume from offsets

``` java
{{ insert('kafka/companion/ConsumerTest.java', 'offsets') }}
```

### Consumer assignment and offsets

During execution of the consumer task, information about the underlying consumer's topic partition assignment, position or committed offsets can be accessed.
``` java
{{ insert('kafka/companion/ConsumerTest.java', 'committed') }}
```

## Registering Custom Serdes

KafkaCompanion handles Serializers and Deserializers for default types such as primitives, String, ByteBuffer, UUID.

Serdes for custom types can be registered to the companion object, and will be used for producer and consumer tasks:

``` java
{{ insert('kafka/companion/KafkaTest.java', 'serdes') }}
```

## Topics

Create, list, describe and delete topics:
``` java
{{ insert('kafka/companion/TopicsTest.java', 'code') }}
```

## Consumer Groups and Offsets

List topic partition offsets:
``` java
{{ insert('kafka/companion/OffsetsTest.java', 'offsets') }}
```

List, describe, alter consumer groups and their offsets:
``` java
{{ insert('kafka/companion/OffsetsTest.java', 'consumerGroups') }}
```
