package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.ChannelUtils.getClientCapabilities;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static java.time.Duration.ofSeconds;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.reactive.messaging.amqp.fault.AmqpAccept;
import io.smallrye.reactive.messaging.amqp.fault.AmqpFailStop;
import io.smallrye.reactive.messaging.amqp.fault.AmqpFailureHandler;
import io.smallrye.reactive.messaging.amqp.fault.AmqpModifiedFailed;
import io.smallrye.reactive.messaging.amqp.fault.AmqpModifiedFailedAndUndeliverableHere;
import io.smallrye.reactive.messaging.amqp.fault.AmqpReject;
import io.smallrye.reactive.messaging.amqp.fault.AmqpRelease;
import io.smallrye.reactive.messaging.amqp.tracing.AmqpOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class IncomingAmqpChannel {

    private final AmqpOpenTelemetryInstrumenter amqpInstrumenter;
    private final Multi<? extends Message<?>> multi;
    private final AtomicBoolean opened;
    private final BiConsumer<String, Throwable> reportFailure;
    private final ConnectionHolder holder;
    private final boolean healthEnabled;

    public IncomingAmqpChannel(AmqpConnectorIncomingConfiguration ic, AmqpClient client, Vertx vertx,
            Instance<OpenTelemetry> openTelemetryInstance, BiConsumer<String, Throwable> reportFailure) {
        this.reportFailure = reportFailure;
        this.opened = new AtomicBoolean(false);
        this.healthEnabled = ic.getHealthEnabled();

        String channel = ic.getChannel();
        String address = ic.getAddress().orElse(channel);
        boolean broadcast = ic.getBroadcast();
        String link = ic.getLinkName().orElse(channel);
        boolean cloudEvents = ic.getCloudEvents();
        boolean tracing = ic.getTracingEnabled();
        Context root = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.holder = new ConnectionHolder(client, ic, vertx, root);

        AmqpReceiverOptions options = new AmqpReceiverOptions()
                .setAutoAcknowledgement(ic.getAutoAcknowledgement())
                .setDurable(ic.getDurable())
                .setLinkName(link)
                .setCapabilities(getClientCapabilities(ic))
                .setSelector(ic.getSelector().orElse(null));

        if (ic.getTracingEnabled()) {
            amqpInstrumenter = AmqpOpenTelemetryInstrumenter.createForConnector(openTelemetryInstance);
        } else {
            amqpInstrumenter = null;
        }

        AmqpFailureHandler onNack = createFailureHandler(ic);
        Integer interval = ic.getRetryOnFailInterval();
        Integer attempts = ic.getRetryOnFailAttempts();
        multi = holder.getOrEstablishConnection()
                .onItem().transformToUni(connection -> connection.createReceiver(address, options))
                .onItem().invoke(r -> opened.set(true))
                .onItem().transformToMulti(r -> getStreamOfMessages(r, holder, address, channel, onNack,
                        cloudEvents, tracing))
                // Retry on failure.
                .onFailure().invoke(log::retrieveMessagesRetrying)
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(interval)).atMost(attempts)
                .onFailure().invoke(t -> {
                    opened.set(false);
                    log.retrieveMessagesNoMoreRetrying(t);
                }).plug(m -> broadcast ? m.broadcast().toAllSubscribers() : m);

    }

    public boolean isOpen() {
        return opened.get();
    }

    public ConnectionHolder getHolder() {
        return holder;
    }

    public Flow.Publisher<? extends Message<?>> getPublisher() {
        return multi;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Multi<? extends Message<?>> getStreamOfMessages(AmqpReceiver receiver,
            ConnectionHolder holder,
            String address,
            String channel,
            AmqpFailureHandler onNack,
            boolean cloudEventEnabled,
            Boolean tracingEnabled) {
        log.receiverListeningAddress(address);

        // The processor is used to inject AMQP Connection failure in the stream and trigger a retry.
        BroadcastProcessor processor = BroadcastProcessor.create();
        receiver.exceptionHandler(t -> {
            log.receiverError(t);
            processor.onError(t);
        });
        holder.onFailure(processor::onError);

        return Multi.createFrom().deferred(
                () -> {
                    Multi<Message<?>> stream = receiver.toMulti()
                            .emitOn(c -> VertxContext.runOnContext(holder.getContext().getDelegate(), c))
                            .onItem().transformToUniAndConcatenate(m -> {
                                try {
                                    return Uni.createFrom().item(new AmqpMessage<>(m, holder.getContext(), onNack,
                                            cloudEventEnabled, tracingEnabled));
                                } catch (Exception e) {
                                    log.unableToCreateMessage(channel, e);
                                    return Uni.createFrom().nullItem();
                                }
                            });

                    if (tracingEnabled) {
                        stream = stream.onItem()
                                .transform(m -> amqpInstrumenter.traceIncoming(m, (AmqpMessage<?>) m));
                    }

                    return Multi.createBy().merging().streams(stream, processor);
                });
    }

    private AmqpFailureHandler createFailureHandler(AmqpConnectorIncomingConfiguration config) {
        String strategy = config.getFailureStrategy();
        AmqpFailureHandler.Strategy actualStrategy = AmqpFailureHandler.Strategy.from(strategy);
        return switch (actualStrategy) {
            case FAIL -> new AmqpFailStop(config.getChannel(), reportFailure);
            case ACCEPT -> new AmqpAccept(config.getChannel());
            case REJECT -> new AmqpReject(config.getChannel());
            case RELEASE -> new AmqpRelease(config.getChannel());
            case MODIFIED_FAILED -> new AmqpModifiedFailed(config.getChannel());
            case MODIFIED_FAILED_UNDELIVERABLE_HERE -> new AmqpModifiedFailedAndUndeliverableHere(config.getChannel());
        };

    }

    public Uni<Boolean> isConnected() {
        return holder.isConnected();
    }

    public long getHealthTimeout() {
        return holder.getHealthTimeout();
    }

    public boolean isHealthEnabled() {
        return healthEnabled;
    }

    public void close() {
        opened.set(false);
    }
}
