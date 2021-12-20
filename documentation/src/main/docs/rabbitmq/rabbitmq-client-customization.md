# Customizing the underlying RabbitMQ client

You can customize the underlying RabbitMQ Client configuration by
*producing* an instance of
[`RabbitMQOptions`](https://vertx.io/docs/apidocs/io/vertx/rabbitmq/RabbitMQOptions.html):

``` java
{{ insert('rabbitmq/customization/RabbitMQProducers.java', 'named') }}
```

This instance is retrieved and used to configure the client used by the
connector. You need to indicate the name of the client using the
`client-options-name` attribute:

    mp.messaging.incoming.prices.client-options-name=my-named-options
