package io.smallrye.reactive.messaging.pulsar;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.health.HealthReporter;
@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
public class PulsarConnector implements IncomingConnectorFactory, OutgoingConnectorFactory, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-pulsar";

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        return null;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        return null;
    }
}
