# Implementation of the MicroProfile Reactive Messaging specification

This project is an implementation of the (next to be) Eclipse MicroProfile Reactive Messaging specification - a CDI 
extension to build data streaming application. It provides support for:

* Apache Kafka
* MQTT
* AMQP 1.0
* Apache Camel

It also provides a way to inject _streams_ into CDI beans, and so link your Reactive Messaging streams into CDI beans, 
or JAX-RS resources.

## Getting started

### Prerequisites 

Unfortunately, right now, there are no release of this project, so you need to build it by yourself. The build process
 requires Apache Maven and Java 8+.  You need to build the SmallRye Reactive Stream Operators implementation first:

```bash
git clone git@github.com:smallrye/smallrye-reactive-streams-operators.git
cd smallrye-reactive-streams-operators
mvn clean install
```

Then, build this repository

```bash
cd $ROOT
mvn clean install
```

### How to start

The best way to start is to look at the `examples/quickstart` project. It's a Maven project listing the minimal set of 
dependencies and containing a single class:

```java
package io.smallrye.reactive.messaging.quickstart;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainerInitializer;

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

* Apache Vert.x
* RX Java 2
* SmallRye Reactive Stream Operators (any implementation would work)
* Weld (any implementation would work)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details, and the process for submitting pull requests.

## Sponsors

The project is sponsored by Red Hat.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.


