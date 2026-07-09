package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.enterprise.inject.Instance;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.jms.commit.JmsCommitHandler;
import io.smallrye.reactive.messaging.jms.fault.JmsFailureHandler;
import io.smallrye.reactive.messaging.jms.tracing.JmsOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.jms.tracing.JmsTrace;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

class JmsSource {

    private final Multi<? extends IncomingJmsMessage<?>> source;
    private final JmsMessagePoller poller;

    private final JmsPublisher publisher;
    private final boolean isTracingEnabled;
    private final JmsOpenTelemetryInstrumenter jmsInstrumenter;

    private final JmsCommitHandler commitHandler;
    private final JmsFailureHandler failureHandler;
    private final List<Throwable> failures = new ArrayList<>();

    JmsSource(Vertx vertx,
            JmsConnectorIncomingConfiguration config,
            Instance<OpenTelemetry> openTelemetryInstance,
            JsonMapping jsonMapping,
            JmsMessagePoller poller,
            Supplier<JmsCommitHandler> commitHandlerSupplier,
            Function<BiConsumer<Throwable, Boolean>, JmsFailureHandler> failureHandlerFunction,
            DemandPauser demandPauser) {
        this.isTracingEnabled = config.getTracingEnabled();
        String channel = config.getChannel();
        boolean retry = config.getRetry();
        this.poller = poller;
        this.commitHandler = commitHandlerSupplier.get();
        this.failureHandler = failureHandlerFunction.apply(this::reportFailure);
        if (isTracingEnabled) {
            jmsInstrumenter = JmsOpenTelemetryInstrumenter.createForSource(openTelemetryInstance);
        } else {
            jmsInstrumenter = null;
        }

        Context rootCtx = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.publisher = new JmsPublisher(channel, poller);

        Multi<? extends Message<jakarta.jms.Message>> pipeline = Multi.createFrom().publisher(publisher)
                .emitOn(rootCtx::runOnContext);
        if (demandPauser != null) {
            pipeline = pipeline.pauseDemand().using(demandPauser);
        }
        this.source = pipeline
                .onItem().invoke(m -> {
                    if (demandPauser != null) {
                        demandPauser.pause();
                    }
                })
                .onItem().transform(m -> {
                    IncomingJmsMessage<?> msg = new IncomingJmsMessage<>(m.getPayload(), jsonMapping, commitHandler,
                            failureHandler);
                    for (Object meta : m.getMetadata()) {
                        msg.injectMetadata(meta);
                    }
                    return msg;
                })
                .onItem().invoke(this::incomingTrace)
                .onFailure(t -> {
                    log.terminalErrorOnChannel(channel);
                    this.poller.close();
                    return retry;
                })
                .retry()
                .withBackOff(Duration.parse(config.getRetryInitialDelay()),
                        Duration.parse(config.getRetryMaxDelay()))
                .withJitter(config.getRetryJitter())
                .atMost(config.getRetryMaxRetries())
                .onFailure()
                .invoke(throwable -> log.terminalErrorRetriesExhausted(config.getChannel(), throwable))
                .plug(m -> config.getBroadcast() ? m.broadcast().toAllSubscribers() : m);
    }

    public synchronized void reportFailure(Throwable failure, boolean fatal) {
        //log.failureReported(topics, failure);
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);

        if (fatal) {
            close();
        }
    }

    void close() {
        publisher.close();
        poller.close();
        commitHandler.close();
        failureHandler.close();
    }

    Multi<? extends IncomingJmsMessage<?>> getSource() {
        return source;
    }

    @SuppressWarnings("PublisherImplementation")
    private static class JmsPublisher implements Flow.Publisher<Message<jakarta.jms.Message>>, Flow.Subscription {

        private final AtomicLong requests = new AtomicLong();
        private final AtomicReference<Flow.Subscriber<? super Message<jakarta.jms.Message>>> downstream = new AtomicReference<>();
        private final ExecutorService executor;
        private final JmsMessagePoller poller;
        private final AtomicBoolean polling = new AtomicBoolean();

        private JmsPublisher(String channel, JmsMessagePoller poller) {
            this.poller = poller;
            this.executor = Executors.newSingleThreadExecutor(new JmsThreadFactory("smallrye-jms-" + channel));
        }

        void close() {
            Flow.Subscriber<? super Message<jakarta.jms.Message>> subscriber = downstream.getAndSet(null);
            if (subscriber != null) {
                subscriber.onComplete();
            }
            executor.shutdown();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super Message<jakarta.jms.Message>> s) {
            if (downstream.compareAndSet(null, s)) {
                s.onSubscribe(this);
            } else {
                Subscriptions.fail(s, ex.illegalStateAlreadySubscriber());
            }
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                add(n);
                ensurePolling();
            }
        }

        private void ensurePolling() {
            if (polling.compareAndSet(false, true)) {
                executor.execute(this::pollLoop);
            }
        }

        private void pollLoop() {
            try {
                Flow.Subscriber<? super Message<jakarta.jms.Message>> sub;
                while (requests.get() > 0 && (sub = downstream.get()) != null) {
                    Message<jakarta.jms.Message> message = poller.poll();
                    if (message != null) {
                        requests.decrementAndGet();
                        sub.onNext(message);
                    }
                }
            } catch (Exception e) {
                log.clientClosed();
                Flow.Subscriber<? super Message<jakarta.jms.Message>> subscriber = downstream.getAndSet(null);
                if (subscriber != null) {
                    subscriber.onError(e);
                }
            } finally {
                polling.set(false);
            }
            if (requests.get() > 0 && downstream.get() != null) {
                ensurePolling();
            }
        }

        @Override
        public void cancel() {
            close();
        }

        long add(long req) {
            for (;;) {
                long r = requests.get();
                if (r == Long.MAX_VALUE) {
                    return Long.MAX_VALUE;
                }
                long u = r + req;
                long v;
                if (u < 0L) {
                    v = Long.MAX_VALUE;
                } else {
                    v = u;
                }
                if (requests.compareAndSet(r, v)) {
                    return v;
                }
            }
        }
    }

    public void incomingTrace(IncomingJmsMessage<?> jmsMessage) {
        if (isTracingEnabled) {
            Optional<IncomingJmsMessageMetadata> metadata = jmsMessage.getMetadata(IncomingJmsMessageMetadata.class);
            Optional<String> queueName = metadata.map(a -> {
                Destination destination = a.getDestination();
                if (destination instanceof Queue queue) {
                    try {
                        return queue.getQueueName();
                    } catch (JMSException e) {
                        return null;
                    }
                }
                return null;
            });
            jakarta.jms.Message unwrapped = jmsMessage.unwrap(jakarta.jms.Message.class);

            Map<String, Object> properties = new HashMap<>();
            try {
                Enumeration<?> propertyNames = unwrapped.getPropertyNames();
                while (propertyNames.hasMoreElements()) {
                    String name = (String) propertyNames.nextElement();
                    Object value = unwrapped.getObjectProperty(name);
                    properties.put(name, value);
                }
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }

            JmsTrace jmsTrace = new JmsTrace.Builder()
                    .withQueue(queueName.orElse(null))
                    .withMessage(unwrapped)
                    .build();

            jmsInstrumenter.traceIncoming(jmsMessage, jmsTrace);
        }
    }

}
