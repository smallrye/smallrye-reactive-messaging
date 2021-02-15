package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

        private final AtomicInteger wip = new AtomicInteger();
        /**
         * Stores the current downstream demands.
         */
        private final AtomicLong requested = new AtomicLong();

        /**
         * Started flag.
         */
        private final AtomicBoolean started = new AtomicBoolean();

        /**
         * Paused / Resumed flag.
         */
        private final AtomicBoolean paused = new AtomicBoolean();

        /**
         * The polling uni to avoid re-assembling a Uni everytime.
         */
        private final Uni<ConsumerRecords<K, V>> pollUni;

        // TODO Make sure we don't enqueue too much, especially if req == Long.MAX
        private final Queue<ConsumerRecord<K, V>> queue;

        /**
         * {@code true} if the subscription has been cancelled.
         */
        private volatile boolean cancelled;

        public KafkaRecordStreamSubscription(
                ReactiveKafkaConsumer<K, V> client,
                KafkaConnectorIncomingConfiguration config,
                MultiSubscriber<? super ConsumerRecord<K, V>> subscriber) {
            this.client = client;
            this.pauseResumeEnabled = config.getPauseIfNoRequests();
            this.downstream = subscriber;
            this.queue = new ConcurrentLinkedDeque<>();
            this.pollUni = client.poll()
                    .onItem().transform(cr -> {
                        if (cr.isEmpty()) {
                            return null;
                        }
                        cr.forEach(queue::offer);
                        return cr;
                    })
                    .plug(m -> {
                        if (config.getRetry()) {
                            long max = config.getRetryAttempts();
                            if (max == -1) {
                                max = Long.MAX_VALUE;
                            }
                            int maxWait = config.getRetryMaxWait();
                            return m.onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(maxWait))
                                    .atMost(max);
                        }
                        return m;
                    });
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                if (!cancelled) {
                    Subscriptions.add(requested, n);
                    if (started.compareAndSet(false, true) && !cancelled) {
                        poll();
                    } else if (!cancelled) {
                        dispatch();
                    }
                }
            } else {
                throw new IllegalArgumentException("Invalid request");
            }

        }

        private void poll() {
            if (cancelled || client.isClosed()) {
                return;
            }

            pollUni
                    .subscribe().with(
                            cr -> {
                                if (cr == null) {
                                    client.executeWithDelay(this::poll, Duration.ofMillis(2))
                                            .subscribe().with(x -> {
                                            }, this::report);
                                } else {
                                    dispatch();
                                    client.runOnPollingThread(c -> {
                                        poll();
                                    })
                                            .subscribe().with(x -> {
                                            }, this::report);
                                }
                            },
                            this::report);
        }

        private void report(Throwable fail) {
            if (!cancelled) {
                cancelled = true;
                downstream.onFailure(fail);
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
            final Queue<ConsumerRecord<K, V>> q = queue;
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
                    if (requests <= queue.size() && paused.compareAndSet(false, true)) {
                        log.pausingChannel(config.getChannel());
                        client.pause()
                                .subscribe().with(x -> {
                                }, this::report);
                    } else if (requests > queue.size() && paused.compareAndSet(true, false)) {
                        log.resumingChannel(config.getChannel());
                        client.resume()
                                .subscribe().with(x -> {
                                }, this::report);
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
            if (cancelled) {
                return;
            }
            cancelled = true;
            if (wip.getAndIncrement() == 0) {
                // nothing was currently dispatched, clearing the queue.
                client.close();
                queue.clear();
            }
        }

        boolean isCancelled() {
            if (cancelled) {
                queue.clear();
                client.close();
                return true;
            }
            return false;
        }
    }
}
