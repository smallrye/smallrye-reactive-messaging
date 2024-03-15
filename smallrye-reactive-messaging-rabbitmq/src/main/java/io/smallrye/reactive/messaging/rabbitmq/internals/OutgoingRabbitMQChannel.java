package io.smallrye.reactive.messaging.rabbitmq.internals;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;
import static io.smallrye.reactive.messaging.rabbitmq.internals.RabbitMQClientHelper.declareExchangeIfNeeded;
import static java.time.Duration.ofSeconds;

import java.util.concurrent.Flow;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.rabbitmq.ClientHolder;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorOutgoingConfiguration;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.mutiny.rabbitmq.RabbitMQPublisher;
import io.vertx.rabbitmq.RabbitMQPublisherOptions;

public class OutgoingRabbitMQChannel {

    private final Flow.Subscriber<Message<?>> subscriber;
    private final RabbitMQConnectorOutgoingConfiguration config;
    private final ClientHolder holder;
    private volatile RabbitMQPublisher publisher;

    public OutgoingRabbitMQChannel(RabbitMQConnector connector, RabbitMQConnectorOutgoingConfiguration oc,
            Instance<OpenTelemetry> openTelemetryInstance) {

        this.config = oc;
        // Create a client
        final RabbitMQClient client = RabbitMQClientHelper.createClient(connector, oc);
        client.getDelegate().addConnectionEstablishedCallback(promise -> {
            // Ensure we create the exchange to which messages are to be sent
            RabbitMQClientHelper.declareExchangeIfNeeded(client, oc, connector.configMaps())
                    .subscribe().with((ignored) -> promise.complete(), promise::fail);
        });

        holder = new ClientHolder(client, oc, connector.vertx(), null);
        final Uni<RabbitMQPublisher> getSender = holder.getOrEstablishConnection()
                .onItem()
                .transformToUni(connection -> Uni.createFrom().item(RabbitMQPublisher.create(connector.vertx(), connection,
                        new RabbitMQPublisherOptions()
                                .setReconnectAttempts(oc.getReconnectAttempts())
                                .setReconnectInterval(ofSeconds(oc.getReconnectInterval()).toMillis())
                                .setMaxInternalQueueSize(oc.getMaxOutgoingInternalQueueSize().orElse(Integer.MAX_VALUE)))))
                // Start the publisher
                .onItem().call(RabbitMQPublisher::start)
                .invoke(publisher -> this.publisher = publisher)
                .onFailure().recoverWithNull().memoize().indefinitely();

        // Set up a sender based on the publisher we established above
        final RabbitMQMessageSender processor = new RabbitMQMessageSender(oc, getSender, openTelemetryInstance);

        // Return a SubscriberBuilder
        subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(t -> log.error(oc.getChannel(), t)));
    }

    public Flow.Subscriber<Message<?>> getSubscriber() {
        return subscriber;
    }

    public HealthReport.HealthReportBuilder isAlive(HealthReport.HealthReportBuilder builder) {
        if (!config.getHealthEnabled()) {
            return builder;
        }

        return computeHealthReport(builder);
    }

    private HealthReport.HealthReportBuilder computeHealthReport(HealthReport.HealthReportBuilder builder) {
        RabbitMQClient client = holder.client();
        if (client == null) {
            return builder.add(new HealthReport.ChannelInfo(config.getChannel(), false));
        }

        boolean ok = true;
        if (holder.hasBeenConnected()) {
            ok = client.isConnected() && client.isOpenChannel();
        }

        return builder.add(new HealthReport.ChannelInfo(config.getChannel(), ok));
    }

    public HealthReport.HealthReportBuilder isReady(HealthReport.HealthReportBuilder builder) {
        if (!config.getHealthEnabled() || !config.getHealthReadinessEnabled()) {
            return builder;
        }

        return computeHealthReport(builder);
    }

    public void terminate() {
        if (publisher != null) {
            publisher.stopAndAwait();
        }
    }
}
