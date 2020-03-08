package io.smallrye.reactive.messaging.jms;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Destination;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.Topic;
import javax.json.bind.Jsonb;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;

class JmsSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsSource.class);
    private final PublisherBuilder<IncomingJmsMessage<?>> source;

    private final JmsPublisher publisher;

    JmsSource(JMSContext context, Config config, Jsonb json, Executor executor) {
        String name = config.getOptionalValue("destination", String.class)
                .orElseGet(() -> config.getValue("channel-name", String.class));

        String selector = config.getOptionalValue("selector", String.class).orElse(null);
        boolean nolocal = config.getOptionalValue("no-local", Boolean.TYPE).orElse(false);
        boolean broadcast = config.getOptionalValue("broadcast", Boolean.TYPE).orElse(false);

        Destination destination = getDestination(context, name, config);

        JMSConsumer consumer;
        if (config.getOptionalValue("durable", Boolean.TYPE).orElse(false)) {
            if (!(destination instanceof Topic)) {
                throw new IllegalArgumentException("Invalid destination, only topic can be durable");
            }
            consumer = context.createDurableConsumer((Topic) destination, name, selector, nolocal);
        } else {
            consumer = context.createConsumer(destination, selector, nolocal);
        }

        publisher = new JmsPublisher(consumer);

        if (!broadcast) {
            source = ReactiveStreams.fromPublisher(publisher).map(m -> new IncomingJmsMessage<>(m, executor, json));
        } else {
            source = ReactiveStreams.fromPublisher(
                    Multi.createFrom().publisher(publisher)
                            .map(m -> new IncomingJmsMessage<>(m, executor, json))
                            .broadcast().toAllSubscribers());
        }
    }

    void close() {
        publisher.close();
    }

    private Destination getDestination(JMSContext context, String name, Config config) {
        String type = config.getOptionalValue("destination-type", String.class).orElse("queue");
        switch (type.toLowerCase()) {
            case "queue":
                LOGGER.info("Creating queue {}", name);
                return context.createQueue(name);
            case "topic":
                LOGGER.info("Creating topic {}", name);
                return context.createTopic(name);
            default:
                throw new IllegalArgumentException("Unknown destination type: " + type);
        }

    }

    PublisherBuilder<IncomingJmsMessage<?>> getSource() {
        return source;
    }

    @SuppressWarnings("PublisherImplementation")
    private static class JmsPublisher implements Publisher<Message>, Subscription {

        private final AtomicLong requests = new AtomicLong();
        private final AtomicReference<Subscriber<? super Message>> downstream = new AtomicReference<>();
        private final JMSConsumer consumer;
        private final ExecutorService executor;
        private boolean unbounded;

        private JmsPublisher(JMSConsumer consumer) {
            this.consumer = consumer;
            this.executor = Executors.newSingleThreadExecutor();
        }

        void close() {
            Subscriber<? super Message> subscriber = downstream.getAndSet(null);
            if (subscriber != null) {
                subscriber.onComplete();
            }
            consumer.close();
            executor.shutdown();
        }

        @Override
        public void subscribe(Subscriber<? super Message> s) {
            if (downstream.compareAndSet(null, s)) {
                s.onSubscribe(this);
            } else {
                Subscriptions.fail(s, new IllegalStateException("There is already a subscriber"));
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
                        LOGGER.warn("Unable to receive JMS messages - client has been closed");
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
