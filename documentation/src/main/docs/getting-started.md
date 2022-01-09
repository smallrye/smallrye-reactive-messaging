# Getting Started

The easiest way to start using SmallRye Reactive Messaging is from [Quarkus](https://quarkus.io).
SmallRye Reactive Messaging can also be used standalone, or with [Open Liberty](https://openliberty.io/guides/microprofile-reactive-messaging.html).

First, go to [code.quarkus.io](https://code.quarkus.io/?g=io.smallrye&a=getting-started-with-reactive-messaging&e=smallrye-reactive-messaging).
Select the `smallrye-reactive-messaging` extension (already done if you use the link), and then click on the generate button to download the code.

One downloaded, unzip the project and import it in your IDE.

If you look at the `pom.xml` file, you will see the following dependency:

```xml
 <dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-smallrye-reactive-messaging</artifactId>
</dependency>
```

It provides the support for SmallRye Reactive Messaging.

Ok, so far so good, but we need event-driven _beans_.


Create the `quickstart` package, and copy the following class into it:

For instance:

```java
{{ insert('quickstart/ReactiveMessagingExample.java') }}
```

This class contains a set of methods:

- producing messages (`source`)
- processing messages (`toUpperCase`)
- transforming the stream by skipping messages (`filter`)
- consuming messages (`sink`)

Each of these methods are connected through *channels*.

Now, let's see this in action.
For the terminal, run:

```shell
> ./mvnw quarkus:dev
```

Running the previous example should give the following output:

```text
>> HELLO
>> SMALLRYE
>> REACTIVE
>> MESSAGE
```

Of course, this is a very simple example.
To go further, let's have a look to the core [concepts](concepts/concepts.md) behind SmallRye Reactive Messaging.

