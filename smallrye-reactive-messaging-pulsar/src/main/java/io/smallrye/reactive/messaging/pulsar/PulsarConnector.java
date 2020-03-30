package io.smallrye.reactive.messaging.pulsar;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.validation.Validation;

import io.smallrye.reactive.messaging.annotations.Emitter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.shade.org.apache.commons.lang3.Validate;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
public class PulsarConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-pulsar";
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnector.class);

    @Inject
    private Instance<Vertx> instanceOfVertx;

    ExecutorService  executorService;

    @PostConstruct
    public void initialize() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    private boolean internalVertxInstance = false;
    private Vertx vertx;

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        return ReactiveStreams.<Message<?>> builder()
            .flatMapCompletionStage(m -> CompletableFuture.completedFuture(m))
            .onError(t -> LOGGER.error("Unable to send message to Pulsar", t))
            .ignore();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        try {
            Consumer<String> consumer = getClient(config).
                                        newConsumer(Schema.STRING)
                                        .topic(config.getValue("topic",String.class))
                                        .subscriptionName(config.getValue("subscription-name",String.class))
                                        .subscribe();
            PulsarSource source = new PulsarSource(consumer);
            return ReactiveStreams.fromPublisher(source);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    private PulsarClient getClient(Config config){
        StringBuilder url = new StringBuilder();
        String host = config.getValue("host",String.class);
        String port = config.getValue("port",String.class);
        url.append("pulsar://").append(host).append(":").append(port);
        try {
            return PulsarClient.builder()
                .serviceUrl(url.toString())
                .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }


}
