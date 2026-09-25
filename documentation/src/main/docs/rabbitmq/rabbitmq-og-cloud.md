# Connecting to managed instances

This section describes the connector configuration to use managed
RabbitMQ instances (hosted on the Cloud).

## Cloud AMQP

To connect to an instance of RabbitMQ hosted on [Cloud
AMQP](https://www.cloudamqp.com/), use the following configuration:

``` properties
rabbitmq-host=host-name
rabbitmq-port=5671
rabbitmq-username=user-name
rabbitmq-password=password
rabbitmq-virtual-host=user-name
rabbitmq-ssl=true
```

You can extract the values from the `AMQPS` url displayed on the
administration portal:

    amqps://user-name:password@host/user-name

## Amazon MQ

[Amazon MQ](https://aws.amazon.com/amazon-mq/) can host RabbitMQ brokers
(as well as AMQP 1.0 brokers). To connect to a RabbitMQ instance hosted
on Amazon MQ, use the following configuration:

``` properties
rabbitmq-host=host-name
rabbitmq-port=5671
rabbitmq-username=user-name
rabbitmq-password=password
rabbitmq-ssl=true
```

You can extract the host value from the `AMQPS` url displayed on the
administration console:

    amqps://foobarbaz.mq.us-east-2.amazonaws.com:5671

The username and password are configured during the broker creation.
