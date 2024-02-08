# Receiving AWS SQS Messages

The AWS SQS connector allows you to receive messages from an AWS SQS queue.

## Receiving messages

Before you start, you need to have an AWS account and a SQS queue created.
To receive messages from an SQS queue, you need to create a method that consumes messages from the queue.
``` properties
mp.messaging.incoming.data.connector=smallrye-sqs
mp.messaging.incoming.data.queue=my-queue
```

Then, your application receives `Message<String>`.
You can consume the payload directly:

``` java
{{ insert('sqs/inbound/SqsStringConsumer.java') }}
```

Or, you can retrieve the `Message<String>`:

``` java
{{ insert('sqs/inbound/SqsMessageStringConsumer.java') }}
```

You also can directly consume the `software.amazon.awssdk.services.sqs.model.Message`:

``` java
{{ insert('sqs/inbound/SqsSdkMessageConsumer.java') }}
```

### Receive message request customizer

The receive message requests sent to AWS SQS can be customized by providing a CDI bean implementation of
{{ javadoc('io.smallrye.reactive.messaging.aws.sqs.SqsReceiveMessageRequestCustomizer', False, 'io.smallrye.reactive/smallrye-reactive-messaging-aws-sqs') }}
and configuring it's identifier using the `receive.request.customizer` connector attribute.

``` java
{{ insert('sqs/inbound/SqsReceiveMessageRequestCustomizerExample.java') }}
```

```properties
mp.messaging.incoming.data.connector=smallrye-sqs
mp.messaging.incoming.data.queue=my-queue
mp.messaging.incoming.data.receive.request.customizer=my-customizer
```

Receive requests failed with retryable exceptions are retried automatically, by setting the failed request id.

### Receive message request pause and resume

The AWS SQS connector fetches messages by continuously sending receive message requests.
If messages are not processed in a timely manner, the connector pauses fetching messages until queued messages are processed.

The pause resume can be disabled using the `receive.request.pause.resume` connector attribute.

```properties
mp.messaging.incoming.data.receive.request.pause.resume=false
```

## Deserialization

The connector converts incoming SQS Messages into Reactive Messaging `Message<T>` instances.

The payload type `T` is determined based on the value of the SQS message attribute `_classname`.

If you send messages with the AWS SQS connector (outbound connector),
the `_classname` attribute is automatically added to the message.
The primitive types are transformed from the string representation to the corresponding Java type.
For objects, if one of the `JsonMapping` modules is present on the classpath,
the connector used that JSON module to deserialize the message body to objects.

If the `_classname` attribute is not present, the payload is deserialized as a `String`.

``` java
{{ insert('sqs/json/SqsJsonMapping.java', 'code') }}
```

## Inbound Metadata

Messages coming from SQS contain an instance of {{ javadoc('io.smallrye.reactive.messaging.aws.sqs.SqsIncomingMetadata', False, 'io.smallrye.reactive/smallrye-reactive-messaging-aws-sqs') }}
in the metadata.

SQS message attributes can be accessed from the metadata either by name or by the `MessageAttributeValue` object.

``` java
{{ insert('sqs/inbound/SqsMetadataExample.java') }}
```

## Acknowledgement

The default strategy for acknowledging AWS SQS Message is to *delete* the message from the queue.
With `ack.delete` set to `false`, the message is not deleted from the queue.

## Configuration Reference

{{ insert('../../../target/connectors/smallrye-sqs-incoming.md') }}

