package io.smallrye.reactive.messaging.pulsar;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
public class PulsarConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-pulsar";
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnector.class);

    @Inject
    private Instance<Vertx> instanceOfVertx;

    ExecutorService executorService;

    @PostConstruct
    public void initialize() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    private boolean internalVertxInstance = false;
    private Vertx vertx;

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        PulsarConsumer<Message> pulsarConsumer = new PulsarConsumer<>(config);
        return pulsarConsumer.getSubscriberBuilder();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        PulsarProducer producer = new PulsarProducer(config);
        return ReactiveStreams.fromPublisher(producer);
    }

}
