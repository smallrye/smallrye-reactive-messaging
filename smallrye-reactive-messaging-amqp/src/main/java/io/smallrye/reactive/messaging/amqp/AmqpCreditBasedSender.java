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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.amqp.tracing.HeaderInjectAdapter;
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
    private final boolean useAnonymousSender;
    private final String configuredAddress;
    private final boolean tracingEnabled;

    public AmqpCreditBasedSender(AmqpConnector connector, ConnectionHolder holder,
            AmqpConnectorOutgoingConfiguration configuration, Uni<AmqpSender> retrieveSender) {
        this.connector = connector;
        this.holder = holder;
        this.retrieveSender = retrieveSender;
        this.configuration = configuration;
        this.durable = configuration.getDurable();
        this.ttl = configuration.getTtl();
        this.useAnonymousSender = configuration.getUseAnonymousSender();
        this.configuredAddress = configuration.getAddress().orElseGet(configuration::getChannel);
        this.tracingEnabled = configuration.getTracingEnabled();
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
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    holder.getContext().runOnContext(() -> {
                        setCreditsAndRequest(sender);
                        future.complete(null);
                    });
                    return Uni.createFrom().completionStage(future);
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
                        return send(sender, message, durable, ttl, configuredAddress, useAnonymousSender, configuration)
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
                    }
                });
            }
        });
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

    private Uni<Message<?>> send(AmqpSender sender, Message<?> msg, boolean durable, long ttl, String configuredAddress,
            boolean isAnonymousSender, AmqpConnectorCommonConfiguration configuration) {
        int retryAttempts = configuration.getReconnectAttempts();
        int retryInterval = configuration.getReconnectInterval();
        io.vertx.mutiny.amqp.AmqpMessage amqp;

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

        String actualAddress = getActualAddress(msg, amqp, configuredAddress, isAnonymousSender);
        if (connector.getClients().isEmpty()) {
            log.messageNoSend(actualAddress);
            return Uni.createFrom().item(msg);
        }

        if (!actualAddress.equals(amqp.address())) {
            amqp.getDelegate().unwrap().setAddress(actualAddress);
        }

        createOutgoingTrace(msg, amqp);

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
        if (tracingEnabled) {
            Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(msg);

            final SpanBuilder spanBuilder = AmqpConnector.TRACER.spanBuilder(amqp.address() + " send")
                    .setSpanKind(SpanKind.PRODUCER);

            if (tracingMetadata.isPresent()) {
                // Handle possible parent span
                final Context parentSpanContext = tracingMetadata.get().getCurrentContext();
                if (parentSpanContext != null) {
                    spanBuilder.setParent(parentSpanContext);
                } else {
                    spanBuilder.setNoParent();
                }
            } else {
                spanBuilder.setNoParent();
            }

            final Span span = spanBuilder.startSpan();
            Scope scope = span.makeCurrent();

            // Set Span attributes
            span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "AMQP 1.0");
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, amqp.address());
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "queue");
            span.setAttribute(SemanticAttributes.MESSAGING_PROTOCOL, "AMQP");
            span.setAttribute(SemanticAttributes.MESSAGING_PROTOCOL_VERSION, "1.0");

            // Set span onto headers
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(Context.current(), amqp, HeaderInjectAdapter.SETTER);
            span.end();
            scope.close();
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
