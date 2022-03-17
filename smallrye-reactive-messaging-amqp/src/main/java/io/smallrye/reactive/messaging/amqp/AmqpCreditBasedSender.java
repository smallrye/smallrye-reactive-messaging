package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static java.time.Duration.ofSeconds;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.amqp.ce.AmqpCloudEventHelper;
import io.smallrye.reactive.messaging.amqp.tracing.AmqpAttributesExtractor;
import io.smallrye.reactive.messaging.amqp.tracing.AmqpMessageTextMapSetter;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.mutiny.amqp.AmqpSender;

public class AmqpCreditBasedSender implements Processor<Message<?>, Message<?>>, Subscription {

    private final ConnectionHolder holder;
    private final Uni<AmqpSender> retrieveSender;
    private final AtomicLong requested = new AtomicLong();
    private final AmqpConnectorOutgoingConfiguration configuration;
    private final AmqpConnector connector;

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

    private final Instrumenter<AmqpMessage<?>, Void> instrumenter;

    private volatile boolean isAnonymous;

    /**
     * A flag tracking if we are retrieving the credits for the sender.
     * It avoids flooding the broker with credit requests.
     */
    private volatile boolean creditRetrievalInProgress = false;

    public AmqpCreditBasedSender(AmqpConnector connector, ConnectionHolder holder,
            AmqpConnectorOutgoingConfiguration configuration, Uni<AmqpSender> retrieveSender) {
        this.connector = connector;
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

        this.retryAttempts = configuration.getReconnectAttempts();
        this.retryInterval = configuration.getReconnectInterval();

        AmqpAttributesExtractor amqpAttributesExtractor = new AmqpAttributesExtractor();
        MessagingAttributesGetter<AmqpMessage<?>, Void> messagingAttributesGetter = amqpAttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<AmqpMessage<?>, Void> builder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, MessageOperation.SEND));

        instrumenter = builder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, MessageOperation.SEND))
                .addAttributesExtractor(amqpAttributesExtractor)
                .buildProducerInstrumenter(AmqpMessageTextMapSetter.INSTANCE);
    }

    @Override
    public void subscribe(
            Subscriber<? super Message<?>> subscriber) {
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
            requested.set(credits);
            log.retrievedCreditsForChannel(configuration.getChannel(), credits);
            subscription.request(credits);
            return credits;
        }
        if (credits == 0L && subscription != Subscriptions.CANCELLED) {
            onNoMoreCredit(sender);
        }
        return 0L;
    }

    @Override
    public void onNext(Message<?> message) {
        if (isCancelled()) {
            return;
        }

        Subscriber<? super Message<?>> subscriber = this.downstream.get();

        retrieveSender
                .onItem().transformToUni(sender -> {
                    try {
                        return send(sender, message, durable, ttl, configuredAddress, isAnonymous)
                                .onItem().transform(m -> Tuple2.of(sender, m));
                    } catch (Exception e) {
                        // Message can be sent - nacking and skipping.
                        message.nack(e);
                        log.serializationFailure(configuration.getChannel(), e);
                        return Uni.createFrom().nullItem();
                    }
                })
                .subscribe().with(
                        tuple -> {
                            if (tuple != null) { // Serialization issue
                                subscriber.onNext(tuple.getItem2());
                                if (requested.decrementAndGet() == 0) { // no more credit, request more
                                    onNoMoreCredit(tuple.getItem1());
                                }
                            }
                        },
                        subscriber::onError);
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
                    .onItem().ignore().andContinueWithNull()
                    .subscribe().with(s -> {
                    }, f -> {
                        downstream.get().onError(f);
                    });
        }
    }

    @Override
    public void cancel() {
        Subscription sub = upstream.getAndSet(Subscriptions.CANCELLED);
        if (sub != null && sub != Subscriptions.CANCELLED) {
            sub.cancel();
        }
    }

    private Uni<Message<?>> send(AmqpSender sender, Message<?> msg, boolean durable, long ttl, String configuredAddress,
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
        if (connector.getClients().isEmpty()) {
            log.messageNoSend(actualAddress);
            return Uni.createFrom().item(msg);
        }

        if (!actualAddress.equals(amqp.address())) {
            amqp.getDelegate().unwrap().setAddress(actualAddress);
        }

        if (tracingEnabled) {
            createOutgoingTrace(msg, amqp);
        }

        log.sendingMessageToAddress(actualAddress);
        return sender.sendWithAck(amqp)
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryInterval)).atMost(retryAttempts)
                .onItemOrFailure().transformToUni((success, failure) -> {
                    if (failure != null) {
                        return Uni.createFrom().completionStage(msg.nack(failure));
                    } else {
                        return Uni.createFrom().completionStage(msg.ack());
                    }
                })
                .onItem().transform(x -> msg);
    }

    private void createOutgoingTrace(Message<?> msg, io.vertx.mutiny.amqp.AmqpMessage amqp) {
        Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(msg);
        AmqpMessage<?> message = new AmqpMessage<>(amqp, null, null, false, true);

        Context parentContext = tracingMetadata.map(TracingMetadata::getCurrentContext).orElse(Context.current());
        Context spanContext;
        Scope scope = null;

        boolean shouldStart = instrumenter.shouldStart(parentContext, message);
        if (shouldStart) {
            try {
                spanContext = instrumenter.start(parentContext, message);
                scope = spanContext.makeCurrent();
                message.injectTracingMetadata(TracingMetadata.with(spanContext, parentContext));
                instrumenter.end(spanContext, message, null, null);
            } finally {
                if (scope != null) {
                    scope.close();
                }
            }
        }
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
