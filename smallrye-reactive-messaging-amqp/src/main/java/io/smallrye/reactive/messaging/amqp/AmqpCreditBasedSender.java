package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static java.time.Duration.ofSeconds;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.reactive.messaging.amqp.ce.AmqpCloudEventHelper;
import io.smallrye.reactive.messaging.amqp.tracing.AmqpOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.mutiny.amqp.AmqpSender;

public class AmqpCreditBasedSender implements Processor<Message<?>, Message<?>>, Subscription {

    private final ConnectionHolder holder;
    private final Uni<AmqpSender> retrieveSender;
    private final AmqpConnectorOutgoingConfiguration configuration;

    private final AtomicReference<Subscription> upstream = new AtomicReference<>();
    private final AtomicReference<Subscriber<? super Message<?>>> downstream = new AtomicReference<>();
    private final AtomicBoolean once = new AtomicBoolean();
    private final boolean durable;
    private final long ttl;
    private final String configuredAddress;
    private final boolean tracingEnabled;
    private final boolean mandatoryCloudEventAttributeSet;
    private final boolean writeCloudEvents;
    private final boolean writeAsBinaryCloudEvent;
    private final int retryAttempts;
    private final int retryInterval;

    private final AmqpOpenTelemetryInstrumenter amqpInstrumenter;

    private volatile boolean isAnonymous;

    /**
     * A flag tracking if we are retrieving the credits for the sender.
     * It avoids flooding the broker with credit requests.
     */
    private volatile boolean creditRetrievalInProgress = false;

    private final long maxInflights;

    public AmqpCreditBasedSender(ConnectionHolder holder,
            AmqpConnectorOutgoingConfiguration configuration,
            Uni<AmqpSender> retrieveSender,
            Instance<OpenTelemetry> openTelemetryInstance) {
        this.holder = holder;
        this.retrieveSender = retrieveSender;
        this.configuration = configuration;
        this.durable = configuration.getDurable();
        this.ttl = configuration.getTtl();
        this.configuredAddress = configuration.getAddress().orElseGet(configuration::getChannel);
        this.tracingEnabled = configuration.getTracingEnabled();
        this.mandatoryCloudEventAttributeSet = configuration.getCloudEventsType().isPresent()
                && configuration.getCloudEventsSource().isPresent();
        this.writeCloudEvents = configuration.getCloudEvents();
        this.writeAsBinaryCloudEvent = configuration.getCloudEventsMode().equalsIgnoreCase("binary");

        this.retryAttempts = configuration.getRetryOnFailAttempts();
        this.retryInterval = configuration.getRetryOnFailInterval();
        this.maxInflights = configuration.getMaxInflightMessages();

        if (tracingEnabled) {
            amqpInstrumenter = AmqpOpenTelemetryInstrumenter.createForSender(openTelemetryInstance);
        } else {
            amqpInstrumenter = null;
        }
    }

    @Override
    public void subscribe(Subscriber<? super Message<?>> subscriber) {
        if (!downstream.compareAndSet(null, subscriber)) {
            Subscriptions.fail(subscriber, ex.illegalStateOnlyOneSubscriberAllowed());
        } else {
            if (upstream.get() != null) {
                subscriber.onSubscribe(this);
            }
        }
    }

    private Uni<AmqpSender> getSenderAndCredits() {
        return retrieveSender
                .onItem().call(sender -> {
                    isAnonymous = configuration.getUseAnonymousSender()
                            .orElseGet(() -> ConnectionHolder.supportAnonymousRelay(sender.connection()));
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    holder.getContext().runOnContext(() -> {
                        setCreditsAndRequest(sender);
                        future.complete(null);
                    });
                    return Uni.createFrom().completionStage(future);
                });
    }

    @CheckReturnValue
    public Uni<Boolean> isConnected() {
        return isConnected(true);
    }

    public int getHealthTimeout() {
        return configuration.getHealthTimeout();
    }

    private Uni<Boolean> isConnected(boolean attemptConnection) {
        return holder.isConnected()
                .chain(ok -> {
                    if (!ok && attemptConnection) {
                        // Retry connection, this normally happen during the "send" call
                        return holder.getOrEstablishConnection()
                                .chain(x -> isConnected(false));
                    }
                    return Uni.createFrom().item(ok);
                });
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        if (this.upstream.compareAndSet(null, subscription)) {
            Subscriber<? super Message<?>> subscriber = downstream.get();
            if (subscriber != null) {
                subscriber.onSubscribe(this);
            }
        } else {
            Subscriber<? super Message<?>> subscriber = downstream.get();
            if (subscriber != null) {
                subscriber.onSubscribe(Subscriptions.CANCELLED);
            }
        }
    }

    /**
     * Must be called on the context having created the AMQP connection
     *
     * @param sender the sender
     * @return the remaining credits
     */
    private long setCreditsAndRequest(AmqpSender sender) {
        long credits = sender.remainingCredits();
        Subscription subscription = upstream.get();
        if (credits != 0L && subscription != Subscriptions.CANCELLED) {
            // Request upfront the sender remaining credits or the max inflights
            long request = maxInflights > 0 ? Math.min(credits, maxInflights) : credits;
            log.retrievedCreditsForChannel(configuration.getChannel(), credits);
            subscription.request(request);
            return credits;
        }
        if (credits == 0L && subscription != Subscriptions.CANCELLED) {
            onNoMoreCredit(sender);
        }
        return 0L;
    }

    void requestUpstream() {
        Subscription subscription = upstream.get();
        if (subscription != null && subscription != Subscriptions.CANCELLED) {
            subscription.request(1);
        }
    }

    @Override
    public void onNext(Message<?> message) {
        if (isCancelled()) {
            return;
        }

        Subscriber<? super Message<?>> subscriber = this.downstream.get();

        try {
            send(message, durable, ttl, configuredAddress, isAnonymous)
                    .subscribe().with(tuple -> {
                        if (tuple != null) { // No serialization issue
                            subscriber.onNext(tuple.getItem1());
                            long remainingCredits = tuple.getItem3();
                            if (remainingCredits == 0) { // no more credit, request more
                                onNoMoreCredit(tuple.getItem2());
                            } else { // keep the request one more message
                                requestUpstream();
                            }
                        } else {
                            requestUpstream();
                        }
                    }, subscriber::onError);
        } catch (Exception e) {
            // Message can be sent - nacking and skipping.
            message.nack(e);
            log.serializationFailure(configuration.getChannel(), e);
            requestUpstream();
        }
    }

    private void onNoMoreCredit(AmqpSender sender) {
        if (!creditRetrievalInProgress) {
            creditRetrievalInProgress = true;
            log.noMoreCreditsForChannel(configuration.getChannel());
            holder.getContext().runOnContext(() -> {
                if (isCancelled()) {
                    return;
                }
                long c = setCreditsAndRequest(sender);
                if (c == 0L) { // still no credits, schedule a periodic retry
                    holder.getVertx().setPeriodic(configuration.getCreditRetrievalPeriod(), id -> {
                        if (setCreditsAndRequest(sender) != 0L || isCancelled()) {
                            // Got our new credits or the application has been terminated,
                            // we cancel the periodic task.
                            holder.getVertx().cancelTimer(id);
                            creditRetrievalInProgress = false;
                        } else {
                            log.stillNoMoreCreditsForChannel(configuration.getChannel());
                        }
                    });
                } else {
                    creditRetrievalInProgress = false;
                }
            });
        }
    }

    private boolean isCancelled() {
        Subscription subscription = upstream.get();
        return subscription == Subscriptions.CANCELLED || subscription == null;
    }

    @Override
    public void onError(Throwable throwable) {
        Subscription sub = upstream.getAndSet(Subscriptions.CANCELLED);
        Subscriber<? super Message<?>> subscriber = this.downstream.get();
        if (sub != null && sub != Subscriptions.CANCELLED && subscriber != null) {
            subscriber.onError(throwable);
        }
    }

    @Override
    public void onComplete() {
        Subscription sub = upstream.getAndSet(Subscriptions.CANCELLED);
        Subscriber<? super Message<?>> subscriber = this.downstream.get();
        if (sub != null && sub != Subscriptions.CANCELLED && subscriber != null) {
            subscriber.onComplete();
        }
    }

    @Override
    public void request(long l) {
        // Delay the retrieval of the sender and the request until we get a request.
        if (!once.getAndSet(true)) {
            getSenderAndCredits()
                    .subscribe().with(s -> {
                    }, f -> downstream.get().onError(f));
        }
    }

    @Override
    public void cancel() {
        Subscription sub = upstream.getAndSet(Subscriptions.CANCELLED);
        if (sub != null && sub != Subscriptions.CANCELLED) {
            sub.cancel();
        }
    }

    private Uni<Tuple3<Message<?>, AmqpSender, Long>> send(Message<?> msg, boolean durable, long ttl, String configuredAddress,
            boolean isAnonymousSender) {
        final io.vertx.mutiny.amqp.AmqpMessage amqp = getMessage(msg, durable, ttl, configuredAddress, isAnonymousSender);
        if (amqp == null) {
            return Uni.createFrom().nullItem();
        }
        if (tracingEnabled) {
            amqpInstrumenter.traceOutgoing(msg, new AmqpMessage<>(amqp, null, null, false, true));
        }
        return retrieveSender.onItem().transformToUni(s -> s.sendWithAck(amqp)
                // We are on Vert.x context that created the client, we can access the remaining credits and update it.
                .replaceWith(() -> Tuple3.<Message<?>, AmqpSender, Long> of(msg, s, s.remainingCredits())))
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryInterval)).atMost(retryAttempts)
                .onItemOrFailure().call((s, failure) -> {
                    if (failure != null) {
                        return Uni.createFrom().completionStage(msg.nack(failure));
                    } else {
                        return Uni.createFrom().completionStage(msg.ack());
                    }
                });
    }

    private io.vertx.mutiny.amqp.AmqpMessage getMessage(Message<?> msg, boolean durable, long ttl, String configuredAddress,
            boolean isAnonymousSender) {
        io.vertx.mutiny.amqp.AmqpMessage amqp;
        OutgoingCloudEventMetadata<?> ceMetadata = msg.getMetadata(OutgoingCloudEventMetadata.class)
                .orElse(null);

        if (msg instanceof AmqpMessage) {
            amqp = ((AmqpMessage<?>) msg).getAmqpMessage();
        } else if (msg.getPayload() instanceof io.vertx.mutiny.amqp.AmqpMessage) {
            amqp = (io.vertx.mutiny.amqp.AmqpMessage) msg.getPayload();
        } else if (msg.getPayload() instanceof io.vertx.amqp.AmqpMessage) {
            amqp = new io.vertx.mutiny.amqp.AmqpMessage((io.vertx.amqp.AmqpMessage) msg.getPayload());
        } else if (msg.getPayload() instanceof org.apache.qpid.proton.message.Message) {
            org.apache.qpid.proton.message.Message message = (org.apache.qpid.proton.message.Message) msg.getPayload();
            AmqpMessageImpl vertxMessage = new AmqpMessageImpl(message);
            amqp = new io.vertx.mutiny.amqp.AmqpMessage(vertxMessage);
        } else {
            amqp = AmqpMessageConverter.convertToAmqpMessage(msg, durable, ttl);
        }

        if (writeCloudEvents && (ceMetadata != null || mandatoryCloudEventAttributeSet)) {
            // We encode the outbound record as Cloud Events if:
            // - cloud events are enabled -> writeCloudEvents
            // - the incoming message contains Cloud Event metadata (OutgoingCloudEventMetadata -> ceMetadata)
            // - or if the message does not contain this metadata, the type and source are configured on the channel
            if (writeAsBinaryCloudEvent) {
                amqp = AmqpCloudEventHelper.createBinaryCloudEventMessage(amqp, ceMetadata, this.configuration);
            } else {
                amqp = AmqpCloudEventHelper.createStructuredEventMessage(amqp, ceMetadata, this.configuration);
            }
        }

        String actualAddress = getActualAddress(msg, amqp, configuredAddress, isAnonymousSender);
        if (!once.get()) {
            log.messageNoSend(actualAddress);
            return null;
        }

        if (!actualAddress.equals(amqp.address())) {
            amqp.getDelegate().unwrap().setAddress(actualAddress);
        }

        log.sendingMessageToAddress(actualAddress);
        return amqp;
    }

    private String getActualAddress(Message<?> message, io.vertx.mutiny.amqp.AmqpMessage amqp, String configuredAddress,
            boolean isAnonymousSender) {
        String address = amqp.address();
        if (address != null) {
            if (isAnonymousSender) {
                return address;
            } else {
                log.unableToUseAddress(address, configuredAddress);
                return configuredAddress;
            }
        }

        return message.getMetadata(OutgoingAmqpMetadata.class)
                .flatMap(o -> {
                    String addressFromMessage = o.getAddress();
                    if (addressFromMessage != null && !isAnonymousSender) {
                        log.unableToUseAddress(addressFromMessage, configuredAddress);
                        return Optional.empty();
                    }
                    return Optional.ofNullable(addressFromMessage);
                })
                .orElse(configuredAddress);
    }

}
