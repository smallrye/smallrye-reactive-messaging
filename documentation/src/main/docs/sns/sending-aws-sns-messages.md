# Sending AWS SNS Messages

The AWS SNS connector allows you to send messages to an AWS SNS topic.

## Sending messages

Before you start, you need to have an AWS account and an SNS topic created.
To send messages to an SNS topic, you need to create a method that produces messages to the topic.

Then, your application can send `Message<String>` to the prices channel.
It can use `String` payloads as in the following snippet:

``` java
{{ insert('sns/outbound/SnsProducer.java') }}
```

Or, you can send `Message<Double>`, which affords the opportunity to
explicitly specify metadata on the outgoing message:

``` java
{{ insert('sns/outbound/SnsMessageProducer.java') }}
```

## Sending messages in batch

You can configure the outbound channel to send messages in batch of maximum 10 messages (AWS SNS limitation).

You can customize the size of batches, `10` being the default batch size, and the delay to wait for new messages to be added to the batch, 3000ms being the default delay:

``` java
mp.messaging.outgoing.prices.connector=smallrye-sns
mp.messaging.outgoing.prices.topic=prices
mp.messaging.outgoing.prices.batch=true
mp.messaging.outgoing.prices.batch-size=5
mp.messaging.outgoing.prices.batch-delay=3000
```

## Serialization

When sending a `Message<T>`, the connector converts the message into a AWS SNS Message.
How the message is converted depends on the payload type:

- If the payload is of type `PublishRequest` it is sent as is.
- If the payload is of type `PublishRequest.Builder`, the topic url is set and sent.
- If the payload is of primitive types the payload is converted to String and the message attribute `_classname` is set to the class name of the payload.
- If the payload is of any other object type, the payload is serialized (using the `JsonMapping` implementation discovered) and the message attribute `_classname` is set to the class name of the payload.

If the message payload cannot be serialized to JSON, the message is *nacked*.

## Outbound Metadata

When sending `Messages`, you can add an instance of {{ javadoc('io.smallrye.reactive.messaging.aws.sns.SnsOutboundMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-aws-sns') }}
to influence how the message is handled by AWS SNS. For example, you can configure the routing key, timestamp and headers:

``` java
{{ insert('sns/outbound/SnsOutboundMetadataExample.java', 'code') }}
```

## Acknowledgement

By default, the Reactive Messaging `Message` is acknowledged when the
send message request is successful. If the message is not sent successfully, the message is *nacked*.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-sns-outgoing.md') }}

