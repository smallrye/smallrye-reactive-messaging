# AWS SNS Connector

!!! warning "Preview"
    The AWS SNS Connector is currently in **preview**.

The AWS SNS Connector adds support for AWS SNS to Reactive Messaging.

With this connector, your application can send messages to a AWS SNS topic.

The AWS SNS connector is based on the [AWS SDK for Java V2](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html).

## Using the AWS SNS connector

To use the AWS SNS Connector, add the following dependency to your project:

``` xml
<dependency>
  <groupId>io.smallrye.reactive</groupId>
  <artifactId>smallrye-reactive-messaging-aws-sns</artifactId>
  <version>{{ attributes['project-version'] }}</version>
</dependency>
```

The connector name is: `smallrye-sns`.

So, to indicate that a channel is managed by this connector you need:
```properties
# Inbound
mp.messaging.incoming.[channel-name].connector=smallrye-sns

# Outbound
mp.messaging.outgoing.[channel-name].connector=smallrye-sns
```

## Configuration

When available the AWS SNS connector discovers the SNS client as a CDI bean.
This allows using the connector with the [Quarkiverse AWS SNS extension](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-sns.html).
If the SNS client is not available as a CDI bean, the connector creates a new client using the provided configuration.

- `topic` - The name of the SNS queue, defaults to channel name if not provided.
- `region` - The name of the SNS region.
- `endpoint-override` - The endpoint url override.
- `credentials-provider` - The fully qualified class name of the credential provider to be used in the client, if not provided the default provider chain is used.

## Additional Resources

- [AWS SNS Documentation](https://docs.aws.amazon.com/sns/index.html)
- [Quarkiverse AWS SNS Extension](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-sns.html)
