[![Maven Central](https://img.shields.io/maven-central/v/io.smallrye.reactive/smallrye-reactive-messaging)](https://search.maven.org/search?q=a:smallrye-reactive-messaging)
[![Continuous Integration Build](https://github.com/smallrye/smallrye-reactive-messaging/workflows/Continuous%20Integration%20Build/badge.svg)](https://github.com/smallrye/smallrye-reactive-messaging/actions)
[![License](https://img.shields.io/github/license/smallrye/smallrye-reactive-messaging.svg)](http://www.apache.org/licenses/LICENSE-2.0)

# Implementation of the MicroProfile Reactive Messaging specification

This project is an implementation of the (next to be) [Eclipse MicroProfile Reactive Messaging](https://github.com/eclipse/microprofile-reactive-messaging) specification - a CDI
extension to build event-driven microservices and data streaming applications. It provides support for:

* [Apache Kafka](https://kafka.apache.org/)
* [MQTT](http://mqtt.org/)
* [AMQP](https://www.amqp.org/) 1.0
* [Apache Camel](https://camel.apache.org/)
* And more!

It also provides a way to inject _streams_ into CDI beans, and so link your [Reactive Messaging streams](https://github.com/eclipse/microprofile-reactive-streams-operators)
into [CDI](http://www.cdi-spec.org/) beans,or [JAX-RS](https://github.com/eclipse-ee4j/jaxrs-api) resources.


## Branches

* main - 3.x development stream. Uses Vert.x 4.x and Microprofile 4.x
* 2.x - Not under development anymore. Uses Vert.x 3.x and Microprofile 3.x

## Getting started

### Prerequisites

See [PREREQUISITES.md](PREREQUISITES.md) for details.

The build process requires Apache Maven and Java 8+ and can be performed using:

```bash
mvn clean install
```

### How to start

The best way to start is to look at the `examples/quickstart` project. It's a Maven project listing the minimal set of
dependencies and containing a single class:

```java
package io.smallrye.reactive.messaging.quickstart;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainerInitializer;

@ApplicationScoped
public class QuickStart {

  public static void main(String[] args) {
    SeContainerInitializer.newInstance().initialize();
  }


  @Outgoing("source")
  public PublisherBuilder<String> source() {
    return ReactiveStreams.of("hello", "with", "SmallRye", "reactive", "message");
  }

  @Incoming("source")
  @Outgoing("processed-a")
  public String toUpperCase(String payload) {
    return payload.toUpperCase();
  }

  @Incoming("processed-a")
  @Outgoing("processed-b")
  public PublisherBuilder<String> filter(PublisherBuilder<String> input) {
    return input.filter(item -> item.length() > 4);
  }

  @Incoming("processed-b")
  public void sink(String word) {
    System.out.println(">> " + word);
  }

}
```

Run the project with: `mvn compile exec:java -Dexec.mainClass=io.smallrye.reactive.messaging.quickstart.QuickStart`:

```bash
>> HELLO
>> SMALLRYE
>> REACTIVE
>> MESSAGE
```

## Built With

* [Eclipse Vert.x](https://vertx.io/)
* [SmallRye Mutiny](https://github.com/smallrye/smallrye-mutiny)
* [SmallRye Reactive Stream Operators](https://github.com/smallrye/smallrye-reactive-streams-operators) (any implementation would work)
* [Weld](https://weld.cdi-spec.org/) (any implementation would work)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details, and the process for submitting pull requests.

## Sponsors

The project is sponsored by [Red Hat](https://www.redhat.com).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.


