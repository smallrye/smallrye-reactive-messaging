# AWS SQS Connector

!!! warning "Preview"
    The AWS SQS Connector is currently in **preview**.

The AWS SQS Connector adds support for AWS SQS to Reactive Messaging.

With this connector, your application can:

-   receive messages from a RabbitMQ queue
-   send messages to a RabbitMQ exchange

The AWS SQS connector is based on the [AWS SDK for Java V2](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html).

## Using the AWS SQS connector

To use the AWS SQS Connector, add the following dependency to your project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-aws-sqs</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-sqs`.

So, to indicate that a channel is managed by this connector you need:
```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-sqs

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-sqs
```

## Configuration

When available the AWS SQS connector discovers the SQS client as a CDI bean.
This allows using the connector with the [Quarkiverse AWS SQS extension](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-sqs.html).
If the SQS client is not available as a CDI bean, the connector creates a new client using the provided configuration.

- `queue` - The name of the SQS queue, defaults to channel name if not provided.
- `region` - The name of the SQS region.
- `endpoint-override` - The endpoint url override.
- `credentials-provider` - The fully qualified class name of the credential provider to be used in the client, if not provided the default provider chain is used.

## Additional Resources

- [AWS SQS Documentation](https://docs.aws.amazon.com/sqs/index.html)
- [Quarkiverse AWS SQS Extension](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-sqs.html)
