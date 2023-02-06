package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Destination;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.Topic;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.reactive.messaging.json.JsonMapping;

class JmsSource {

    private final Flow.Publisher<IncomingJmsMessage<?>> source;

    private final JmsPublisher publisher;

    JmsSource(JMSContext context, JmsConnectorIncomingConfiguration config, JsonMapping jsonMapping, Executor executor) {
        String name = config.getDestination().orElseGet(config::getChannel);
        String selector = config.getSelector().orElse(null);
        boolean nolocal = config.getNoLocal();
        boolean broadcast = config.getBroadcast();
        boolean durable = config.getDurable();

        Destination destination = getDestination(context, name, config);

        JMSConsumer consumer;
        if (durable) {
            if (!(destination instanceof Topic)) {
                throw ex.illegalArgumentInvalidDestination();
            }
            consumer = context.createDurableConsumer((Topic) destination, name, selector, nolocal);
        } else {
            consumer = context.createConsumer(destination, selector, nolocal);
        }

        publisher = new JmsPublisher(consumer);

        Multi<IncomingJmsMessage<?>> multi = Multi.createFrom().publisher(publisher)
                .map(m -> new IncomingJmsMessage<>(m, executor, jsonMapping));
        if (!broadcast) {
            source = multi;
        } else {
            source = multi.broadcast().toAllSubscribers();
        }
    }

    void close() {
        publisher.close();
    }

    private Destination getDestination(JMSContext context, String name, JmsConnectorIncomingConfiguration config) {
        String type = config.getDestinationType();
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

    Flow.Publisher<IncomingJmsMessage<?>> getSource() {
        return source;
    }

    @SuppressWarnings("PublisherImplementation")
    private static class JmsPublisher implements Flow.Publisher<Message>, Flow.Subscription {

        private final AtomicLong requests = new AtomicLong();
        private final AtomicReference<Flow.Subscriber<? super Message>> downstream = new AtomicReference<>();
        private final JMSConsumer consumer;
        private final ExecutorService executor;
        private boolean unbounded;

        private JmsPublisher(JMSConsumer consumer) {
            this.consumer = consumer;
            this.executor = Executors.newSingleThreadExecutor();
        }

        void close() {
            Flow.Subscriber<? super Message> subscriber = downstream.getAndSet(null);
            if (subscriber != null) {
                subscriber.onComplete();
            }
            consumer.close();
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
                        Message message = consumer.receive();
                        if (message != null) { // null means closed.
                            requests.decrementAndGet();
                            downstream.get().onNext(message);
                        }
                    } catch (IllegalStateRuntimeException e) {
                        log.clientClosed();
                    }

                });
            }
        }

        private void startUnboundedReception() {
            consumer.setMessageListener(m -> downstream.get().onNext(m));
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
