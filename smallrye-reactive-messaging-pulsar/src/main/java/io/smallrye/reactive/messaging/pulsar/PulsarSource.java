package io.smallrye.reactive.messaging.pulsar;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.IllegalStateRuntimeException;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.helpers.Subscriptions;

public class PulsarSource<Message> implements Publisher<Message>, Subscription {

    private final AtomicLong requests = new AtomicLong();
    private final AtomicReference<Subscriber<? super Message>> downstream = new AtomicReference<>();
    private final ExecutorService executor;
    private final Consumer consumer;
    private boolean unbounded;
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSource.class);

    public PulsarSource(Consumer consumer) {
        this.consumer = consumer;
        this.executor = Executors.newSingleThreadExecutor();
    }

    void close() {
        Subscriber<? super Message> subscriber = downstream.getAndSet(null);
        if (subscriber != null) {
            subscriber.onComplete();
        }
        consumer.closeAsync();
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
                    Message message = (Message) consumer.receive();
                    if (message != null) { // null means closed.
                        requests.decrementAndGet();
                        downstream.get().onNext(message);
                    }
                } catch (IllegalStateRuntimeException e) {
                    LOGGER.warn("Unable to receive JMS messages - client has been closed");
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }

            });
        }
    }

    private void startUnboundedReception() {
        //TODO: Rafael : don't know what to do here
        //consumer.setMessageListener(m -> downstream.get().onNext(m));
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
