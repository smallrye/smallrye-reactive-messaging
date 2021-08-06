package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.vertx.core.Context;

public class KafkaRecordStream<K, V> extends AbstractMulti<ConsumerRecord<K, V>> {
    private static final int STATE_NEW = 0; // no request yet -- we start polling on the first request
    private static final int STATE_POLLING = 1;
    private static final int STATE_PAUSED = 2;
    private static final int STATE_CANCELLED = 3;

    private final ReactiveKafkaConsumer<K, V> client;
    private final KafkaConnectorIncomingConfiguration config;
    private final Context context;

    public KafkaRecordStream(ReactiveKafkaConsumer<K, V> client,
            KafkaConnectorIncomingConfiguration config, Context context) {
        this.config = config;
        this.client = client;
        this.context = context;
    }

    @Override
    public void subscribe(
            MultiSubscriber<? super ConsumerRecord<K, V>> subscriber) {
        KafkaRecordStreamSubscription subscription = new KafkaRecordStreamSubscription(client, config, subscriber);
        subscriber.onSubscribe(subscription);
    }

    private class KafkaRecordStreamSubscription implements Subscription {
        private final ReactiveKafkaConsumer<K, V> client;
        private final MultiSubscriber<? super ConsumerRecord<K, V>> downstream;
        private final boolean pauseResumeEnabled;

        /**
         * Current state: new (no request yet), polling, paused, cancelled
         */
        private final AtomicInteger state = new AtomicInteger(STATE_NEW);

        private final AtomicInteger wip = new AtomicInteger();
        /**
         * Stores the current downstream demands.
         */
        private final AtomicLong requested = new AtomicLong();

        /**
         * The polling uni to avoid re-assembling a Uni everytime.
         */
        private final Uni<ConsumerRecords<K, V>> pollUni;

        private final int maxQueueSize;
        private final int halfMaxQueueSize;
        private final RecordQueue<ConsumerRecord<K, V>> queue;
        private final long retries;

        public KafkaRecordStreamSubscription(
                ReactiveKafkaConsumer<K, V> client,
                KafkaConnectorIncomingConfiguration config,
                MultiSubscriber<? super ConsumerRecord<K, V>> subscriber) {
            this.client = client;
            this.pauseResumeEnabled = config.getPauseIfNoRequests();
            this.downstream = subscriber;
            // Kafka also defaults to 500, but doesn't have a constant for it
            int batchSize = config.config().getOptionalValue(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.class).orElse(500);
            this.maxQueueSize = batchSize * 2;
            this.halfMaxQueueSize = batchSize;
            // we can exceed maxQueueSize by at most 1 batchSize
            this.queue = new RecordQueue<>(3 * batchSize);
            this.retries = config.getRetryAttempts() == -1 ? Long.MAX_VALUE : config.getRetryAttempts();
            this.pollUni = client.poll()
                    .onItem().transform(cr -> {
                        if (cr.isEmpty()) {
                            return null;
                        }
                        if (log.isTraceEnabled()) {
                            log.tracef("Adding %s messages to the queue", cr.count());
                        }
                        queue.addAll(cr);
                        return cr;
                    })
                    .plug(m -> {
                        if (config.getRetry()) {
                            int maxWait = config.getRetryMaxWait();
                            return m.onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                                    .atMost(retries);
                        }
                        return m;
                    });
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                boolean cancelled = state.get() == STATE_CANCELLED;
                if (!cancelled) {
                    Subscriptions.add(requested, n);
                    if (state.compareAndSet(STATE_NEW, STATE_POLLING)) {
                        poll();
                    } else {
                        dispatch();
                    }
                }
            } else {
                throw new IllegalArgumentException("Invalid request");
            }

        }

        private void poll() {
            if (state.get() != STATE_POLLING || client.isClosed()) {
                return;
            }

            pollUni.subscribe().with(cr -> {
                if (cr == null) {
                    client.executeWithDelay(this::poll, Duration.ofMillis(2))
                            .subscribe().with(this::emptyConsumer, this::report);
                } else {
                    dispatch();

                    int size = queue.size();
                    if (pauseResumeEnabled && size >= maxQueueSize && state.compareAndSet(STATE_POLLING, STATE_PAUSED)) {
                        log.pausingChannel(config.getChannel(), size, maxQueueSize);
                        client.pause()
                                .subscribe().with(this::emptyConsumer, this::report);
                    } else {
                        client.runOnPollingThread(c -> {
                            poll();
                        })
                                .subscribe().with(this::emptyConsumer, this::report);
                    }
                }
            }, this::report);
        }

        private <T> void emptyConsumer(T ignored) {
        }

        private void report(Throwable fail) {
            while (true) {
                int state = this.state.get();
                if (state == STATE_CANCELLED) {
                    break;
                }
                if (this.state.compareAndSet(state, STATE_CANCELLED)) {
                    downstream.onFailure(fail);
                    break;
                }
            }
        }

        void dispatch() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            context.runOnContext(ignored -> run());
        }

        private void run() {
            int missed = 1;
            final RecordQueue<ConsumerRecord<K, V>> q = queue;
            long emitted = 0;
            long requests = requested.get();
            for (;;) {
                if (isCancelled()) {
                    return;
                }

                while (emitted != requests) {
                    ConsumerRecord<K, V> item = q.poll();

                    if (item == null || isCancelled()) {
                        break;
                    }

                    downstream.onItem(item);
                    emitted++;
                }

                requests = requested.addAndGet(-emitted);
                emitted = 0;

                if (pauseResumeEnabled) {
                    int size = q.size();
                    if (log.isTraceEnabled() && state.get() == STATE_POLLING) {
                        log.tracef("In polling state, %s messages in the queue", size);
                    }
                    if (size <= halfMaxQueueSize && state.compareAndSet(STATE_PAUSED, STATE_POLLING)) {
                        log.resumingChannel(config.getChannel(), size, halfMaxQueueSize);
                        client.resume()
                                .subscribe().with(this::emptyConsumer, this::report);
                        poll();
                    }
                }

                int w = wip.get();
                if (missed == w) {
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }

        @Override
        public void cancel() {
            while (true) {
                int state = this.state.get();
                if (state == STATE_CANCELLED) {
                    break;
                }
                if (this.state.compareAndSet(state, STATE_CANCELLED)) {
                    if (wip.getAndIncrement() == 0) {
                        // nothing was currently dispatched, clearing the queue.
                        client.close();
                        queue.clear();
                    }
                    break;
                }
            }
        }

        boolean isCancelled() {
            if (state.get() == STATE_CANCELLED) {
                queue.clear();
                client.close();
                return true;
            }
            return false;
        }
    }
}
