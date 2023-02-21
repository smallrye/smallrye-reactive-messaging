# Apache Camel Connector

The Camel connector adds support for Apache Camel to Reactive Messaging.

[Camel](https://camel.apache.org/) is an open source integration
framework let you integrate various systems consuming or producing data.
Camel implements the Enterprise Integration Patterns and provides
several hundred of components used to access databases, message queues,
APIs or basically [anything under the
sun](https://camel.apache.org/components/latest/).

!!!important
    Smallrye Reactive Messaging 4.x moved from `javax` to `jakarta` APIs therefore only supports Camel 4 releases.
    Similarly, this connector drops the Java 11 support as this is the case for Camel 4.

## Introduction

Camel is not a messaging broker. But, it allows your Reactive Messaging
application to retrieve data from almost anything and send data to
almost anything.

So if you want to send Reactive Messaging `Message` to Telegram or
retrieve data from Salesforce or SAP, this is the connector you need.

One of the Camel cornerstone is the `endpoint` and its `uri` encoding
the connection to an external system. For example,
`file:orders/?delete=true&charset=utf-8` instructs Camel to read the
files from the `orders` directory. URI format and parameters are listed
on the component documentation, such as the [File
component](https://camel.apache.org/components/latest/file-component.html).

## Using the camel connector

To you the camel Connector, add the following dependency to your
project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-camel</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

You will also need the dependency of the Camel component you are using.
For example, if you want to process files, you would need to add the
Camel File Component artifact:

``` xml
<dependency>
  <groupId>org.apache.camel</groupId>
  <artifactId>camel-file</artifactId>
  <version>{{ attributes['camel-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-camel`.

So, to indicate that a channel is managed by this connector you need:

```
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-camel

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-camel
```

