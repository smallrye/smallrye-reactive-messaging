package io.smallrye.reactive.messaging.kafka;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(KafkaConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "bootstrap.servers", alias = "kafka.bootstrap.servers", type = "string", defaultValue = "localhost:9092", direction = Direction.INCOMING_AND_OUTGOING, description = "A comma-separated list of host:port to use for establishing the initial connection to the Kafka cluster.")
@ConnectorAttribute(name = "topic", type = "string", direction = Direction.INCOMING_AND_OUTGOING, description = "The consumed / populated Kafka topic. If not set, the channel name is used")
@ConnectorAttribute(name = "key.deserializer", type = "string", direction = Direction.INCOMING, description = "The deserializer classname used to deserialize the record's key", defaultValue = "org.apache.kafka.common.serialization.StringDeserializer")
@ConnectorAttribute(name = "value.deserializer", type = "string", direction = Direction.INCOMING, description = "The deserializer classname used to deserialize the record's value", mandatory = true)
@ConnectorAttribute(name = "fetch.min.bytes", type = "int", direction = Direction.INCOMING, description = "The minimum amount of data the server should return for a fetch request. The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch request times out waiting for data to arrive.", defaultValue = "1")
@ConnectorAttribute(name = "group.id", type = "string", direction = Direction.INCOMING, description = "A unique string that identifies the consumer group the application belongs to. If not set, a unique, generated id is used")
@ConnectorAttribute(name = "retry", type = "boolean", direction = Direction.INCOMING, description = "Whether or not the connection to the broker is re-attempted in case of failure", defaultValue = "true")
@ConnectorAttribute(name = "retry-attempts", type = "int", direction = Direction.INCOMING, description = "The maximum number of reconnection before failing. -1 means infinite retry", defaultValue = "-1")
@ConnectorAttribute(name = "retry-max-wait", type = "int", direction = Direction.INCOMING, description = "The max delay (in seconds) between 2 reconnects", defaultValue = "30")
@ConnectorAttribute(name = "broadcast", type = "boolean", direction = Direction.INCOMING, description = "Whether the Kafka records should be dispatched to multiple consumer", defaultValue = "false")
@ConnectorAttribute(name = "auto.offset.reset", type = "string", direction = Direction.INCOMING, description = "What to do when there is no initial offset in Kafka.Accepted values are earliest, latest and none", defaultValue = "latest")
@ConnectorAttribute(name = "key.serializer", type = "string", direction = Direction.OUTGOING, description = "The serializer classname used to serialize the record's key", defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
@ConnectorAttribute(name = "value.serializer", type = "string", direction = Direction.OUTGOING, description = "The serializer classname used to serialize the payload", mandatory = true)
@ConnectorAttribute(name = "acks", type = "string", direction = Direction.OUTGOING, description = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the durability of records that are sent. Accepted values are: 0, 1, all", defaultValue = "1")
@ConnectorAttribute(name = "buffer.memory", type = "long", direction = Direction.OUTGOING, description = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server.", defaultValue = "33554432")
@ConnectorAttribute(name = "retries", type = "long", direction = Direction.OUTGOING, description = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.", defaultValue = "2147483647")
@ConnectorAttribute(name = "key", type = "string", direction = Direction.OUTGOING, description = "A key to used when writing the record")
@ConnectorAttribute(name = "partition", type = "int", direction = Direction.OUTGOING, description = "The target partition id. -1 to let the client determine the partition", defaultValue = "-1")
@ConnectorAttribute(name = "waitForWriteCompletion", type = "boolean", direction = Direction.OUTGOING, description = "Whether the client waits for Kafka to acknowledge the written record before acknowledging the message", defaultValue = "true")
public class KafkaConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-kafka";

    @Inject
    private Instance<Vertx> instanceOfVertx;

    private final List<KafkaSource<?, ?>> sources = new CopyOnWriteArrayList<>();
    private final List<KafkaSink> sinks = new CopyOnWriteArrayList<>();

    private boolean internalVertxInstance = false;
    private Vertx vertx;

    public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
        sources.forEach(KafkaSource::closeQuietly);
        sinks.forEach(KafkaSink::closeQuietly);

        if (internalVertxInstance) {
            vertx.closeAndAwait();
        }
    }

    @PostConstruct
    void init() {
        if (instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
        } else {
            this.vertx = instanceOfVertx.get();
        }
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        KafkaSource<Object, Object> source = new KafkaSource<>(vertx, ic);
        sources.add(source);
        return source.getSource();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(config);
        KafkaSink sink = new KafkaSink(vertx, oc);
        sinks.add(sink);
        return sink.getSink();
    }
}
