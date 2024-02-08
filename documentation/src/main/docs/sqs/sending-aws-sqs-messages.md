# Sending AWS SQS Messages

The AWS SQS connector allows you to send messages to an AWS SQS queue.

## Sending messages

Before you start, you need to have an AWS account and an SQS queue created.
To send messages to an SQS queue, you need to create a method that produces messages to the queue.

Then, your application can send `Message<String>` to the prices channel.
It can use `String` payloads as in the following snippet:

``` java
{{ insert('sqs/outbound/SqsStringProducer.java') }}
```

Or, you can send `Message<Double>`, which affords the opportunity to
explicitly specify metadata on the outgoing message:

``` java
{{ insert('sqs/outbound/SqsMessageStringProducer.java') }}
```

## Serialization

When sending a `Message<T>`, the connector converts the message into a AWS SQS Message.
How the message is converted depends on the payload type:

- If the payload is of type `SendMessageRequest` it is sent as is.
- If the payload is of type `SendMessageRequest.Builder`, the queue url is set and sent.
- If the payload is of type `software.amazon.awssdk.services.sqs.model.Message` it is usd to set the message body and attributes.
- If the payload is of primitive types the paylaod is converted to String and the message attribute `_classname` is set to the class name of the payload.
- If the payload is of any other object type, the payload is serialized (using the `JsonMapping` implementation discovered) and the message attribute `_classname` is set to the class name of the payload.

If the message payload cannot be serialized to JSON, the message is *nacked*.

## Outbound Metadata

When sending `Messages`, you can add an instance of {{ javadoc('io.smallrye.reactive.messaging.aws.sqs.SqsOutboundMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-aws-sqs') }}
to influence how the message is handled by AWS SQS. For example, you
can configure the routing key, timestamp and headers:

``` java
{{ insert('sqs/outbound/SqsOutboundMetadataExample.java', 'code') }}
```

## Acknowledgement

By default, the Reactive Messaging `Message` is acknowledged when the
send message request is successful. If the message is not sent successfully, the message is *nacked*.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-sqs-outgoing.md') }}

