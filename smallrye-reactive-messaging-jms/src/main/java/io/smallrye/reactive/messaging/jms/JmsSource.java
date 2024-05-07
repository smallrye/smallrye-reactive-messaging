package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.inject.Instance;
import jakarta.jms.*;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.reactive.messaging.jms.fault.JmsFailureHandler;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

class JmsSource {

    private final Multi<IncomingJmsMessage<?>> source;
    private final JmsResourceHolder<JMSConsumer> resourceHolder;
    private final List<Throwable> failures = new ArrayList<>();

    private final JmsPublisher publisher;
    private final Instance<JmsFailureHandler.Factory> failureHandlerFactories;
    private final JmsConnectorIncomingConfiguration config;
    private final JmsFailureHandler failureHandler;
    private final Context context;

    JmsSource(Vertx vertx, JmsResourceHolder<JMSConsumer> resourceHolder, JmsConnectorIncomingConfiguration config,
            JsonMapping jsonMapping,
            Executor executor, Instance<JmsFailureHandler.Factory> failureHandlerFactories) {
        String channel = config.getChannel();
        final String destinationName = config.getDestination().orElseGet(config::getChannel);
        String selector = config.getSelector().orElse(null);
        boolean nolocal = config.getNoLocal();
        boolean durable = config.getDurable();
        String type = config.getDestinationType();
        boolean retry = config.getRetry();
        this.config = config;
        this.resourceHolder = resourceHolder.configure(r -> getDestination(r.getContext(), destinationName, type),
                r -> {
                    if (durable) {
                        if (!(r.getDestination() instanceof Topic)) {
                            throw ex.illegalArgumentInvalidDestination();
                        }
                        return r.getContext().createDurableConsumer((Topic) r.getDestination(), destinationName, selector,
                                nolocal);
                    } else {
                        return r.getContext().createConsumer(r.getDestination(), selector, nolocal);
                    }
                });
        resourceHolder.getClient();
        this.publisher = new JmsPublisher(resourceHolder);
        this.failureHandlerFactories = failureHandlerFactories;
        this.context = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.failureHandler = createFailureHandler();
        source = Multi.createFrom().publisher(publisher)
                .emitOn(r -> context.runOnContext(r))
                .<IncomingJmsMessage<?>> map(m -> new IncomingJmsMessage<>(m, executor, jsonMapping, failureHandler))
                .onFailure(t -> {
                    log.terminalErrorOnChannel(channel);
                    this.resourceHolder.close();
                    return retry;
                })
                .retry()
                .withBackOff(Duration.parse(config.getRetryInitialDelay()),
                        Duration.parse(config.getRetryMaxDelay()))
                .withJitter(config.getRetryJitter())
                .atMost(config.getRetryMaxRetries())
                .onFailure()
                .invoke(throwable -> log.terminalErrorRetriesExhausted(config.getChannel(), throwable))
                .plug(m -> {
                    if (config.getBroadcast()) {
                        return m.broadcast().toAllSubscribers();
                    }
                    return m;
                });
    }

    void close() {
        publisher.close();
        resourceHolder.close();
    }

    private Destination getDestination(JMSContext context, String name, String type) {
        switch (type.toLowerCase()) {
            case "queue":
                log.creatingQueue(name);
                return context.createQueue(name);
            case "topic":
                log.creatingTopic(name);
                return context.createTopic(name);
            default:
                throw ex.illegalArgumentUnknownDestinationType(type);
        }

    }

    Multi<IncomingJmsMessage<?>> getSource() {
        return source;
    }

    private JmsFailureHandler createFailureHandler() {
        String strategy = config.getFailureStrategy();
        Instance<JmsFailureHandler.Factory> failureHandlerFactory = failureHandlerFactories
                .select(Identifier.Literal.of(strategy));
        if (failureHandlerFactory.isResolvable()) {
            return failureHandlerFactory.get().create(config, this::reportFailure);
        } else {
            throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }
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

    @SuppressWarnings("PublisherImplementation")
    private static class JmsPublisher implements Flow.Publisher<Message>, Flow.Subscription {

        private final AtomicLong requests = new AtomicLong();
        private final AtomicReference<Flow.Subscriber<? super Message>> downstream = new AtomicReference<>();
        private final JmsResourceHolder<JMSConsumer> consumerHolder;
        private final ExecutorService executor;
        private boolean unbounded;

        private JmsPublisher(JmsResourceHolder<JMSConsumer> resourceHolder) {
            this.consumerHolder = resourceHolder;
            this.executor = Executors.newSingleThreadExecutor();
        }

        void close() {
            Flow.Subscriber<? super Message> subscriber = downstream.getAndSet(null);
            if (subscriber != null) {
                subscriber.onComplete();
            }
            executor.shutdown();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super Message> s) {
            if (downstream.compareAndSet(null, s)) {
                s.onSubscribe(this);
            } else {
                Subscriptions.fail(s, ex.illegalStateAlreadySubscriber());
            }
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                boolean u = unbounded;
                if (!u) {
                    long v = add(n);
                    if (v == Long.MAX_VALUE) {
                        unbounded = true;
                        startUnboundedReception();
                    } else {
                        enqueue(n);
                    }
                }
            }
        }

        private void enqueue(long n) {
            for (int i = 0; i < n; i++) {
                executor.execute(() -> {
                    try {
                        Message message = null;
                        while (message == null && downstream.get() != null) {
                            message = consumerHolder.getClient().receive();
                            if (message != null) { // null means closed.
                                requests.decrementAndGet();
                                downstream.get().onNext(message);
                            }
                        }
                    } catch (JMSRuntimeException e) {
                        log.clientClosed();
                        Flow.Subscriber<? super Message> subscriber = downstream.getAndSet(null);
                        if (subscriber != null) {
                            subscriber.onError(e);
                        }
                    }
                });
            }
        }

        private void startUnboundedReception() {
            consumerHolder.getClient().setMessageListener(m -> downstream.get().onNext(m));
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
}
