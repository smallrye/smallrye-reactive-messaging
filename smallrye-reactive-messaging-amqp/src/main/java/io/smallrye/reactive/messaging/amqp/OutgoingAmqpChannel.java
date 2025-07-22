package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.ChannelUtils.getClientCapabilities;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.vertx.amqp.AmqpSenderOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpSender;
import io.vertx.mutiny.core.Vertx;
import io.vertx.proton.ProtonSender;

public class OutgoingAmqpChannel {

    private final AtomicBoolean opened;
    private final Flow.Subscriber<Message<?>> subscriber;
    private final AmqpCreditBasedSender processor;

    public OutgoingAmqpChannel(AmqpConnectorOutgoingConfiguration oc, AmqpClient client, Vertx vertx,
            Instance<OpenTelemetry> openTelemetryInstance, BiConsumer<String, Throwable> reportFailure) {
        String configuredAddress = oc.getAddress().orElseGet(oc::getChannel);

        opened = new AtomicBoolean(false);

        AtomicReference<AmqpSender> sender = new AtomicReference<>();
        String link = oc.getLinkName().orElseGet(oc::getChannel);
        ConnectionHolder holder = new ConnectionHolder(client, oc, vertx, null);

        Uni<AmqpSender> getSender = Uni.createFrom().deferred(() -> {

            // If we already have a sender, use it.
            AmqpSender current = sender.get();
            if (current != null && !current.connection().isDisconnected()) {
                if (isLinkOpen(current)) {
                    return Uni.createFrom().item(current);
                } else {
                    // link closed, close the sender, and recreate one.
                    current.closeAndForget();
                }
            }

            return holder.getOrEstablishConnection()
                    .onItem().transformToUni(connection -> {
                        boolean anonymous = oc.getUseAnonymousSender()
                                .orElseGet(() -> ConnectionHolder.supportAnonymousRelay(connection));

                        if (anonymous) {
                            return connection.createAnonymousSender();
                        } else {
                            return connection.createSender(configuredAddress,
                                    new AmqpSenderOptions()
                                            .setLinkName(link)
                                            .setCapabilities(getClientCapabilities(oc)));
                        }
                    })
                    .onItem().invoke(s -> {
                        AmqpSender orig = sender.getAndSet(s);
                        if (orig != null) { // Close the previous one if any.
                            orig.closeAndForget();
                        }
                        opened.set(true);
                    });
        })
                // If the downstream cancels or on failure, drop the sender.
                .onFailure().invoke(t -> {
                    sender.set(null);
                    opened.set(false);
                })
                .onCancellation().invoke(() -> {
                    sender.set(null);
                    opened.set(false);
                });
        this.processor = new AmqpCreditBasedSender(
                holder,
                oc,
                getSender,
                openTelemetryInstance);

        subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(t -> {
            log.failureReported(oc.getChannel(), t);
            opened.set(false);
            reportFailure.accept(oc.getChannel(), t);
        }));
    }

    private boolean isLinkOpen(AmqpSender current) {
        ProtonSender sender = current.getDelegate().unwrap();
        if (sender == null) {
            return false;
        }
        return sender.isOpen();
    }

    public boolean isOpen() {
        return opened.get();
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    public Uni<Boolean> isConnected() {
        return processor.isConnected();
    }

    public long getHealthTimeout() {
        return processor.getHealthTimeout();
    }

    public void close() {
        processor.cancel();
        opened.set(false);
    }
}
