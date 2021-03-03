package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class RequestProtocolTest extends WeldTestBaseWithoutTails {

    @Test
    public void testRequestProtocolWhenReturningASubscriber() {
        GeneratorApp app = installInitializeAndGet(GeneratorApp.class);

        await().until(() -> app.subscription() != null);

        assertThat(app.list()).isEmpty();

        app.subscription().request(2);

        await().until(() -> app.list().size() == 2);

        app.subscription().request(5);
        await().until(() -> app.list().size() == 7);

        await()
                .pollDelay(Duration.ofMillis(100))
                .until(() -> app.list().size() == 7);

        assertThat(app.list()).contains(1, 2, 3, 4, 5, 6, 7);
        assertThat(app.count()).isEqualTo(9); // request + 1 + 1 (prefetch)
    }

    @SuppressWarnings("ReactiveStreamsSubscriberImplementation")
    @ApplicationScoped
    static class GeneratorApp {

        final AtomicInteger count = new AtomicInteger();
        Subscription subscription;
        final List<Integer> list = new CopyOnWriteArrayList<>();

        @Outgoing("foo")
        public int generate() {
            return count.incrementAndGet();
        }

        public int count() {
            return count.get();
        }

        @Incoming("foo")
        Subscriber<Integer> consume() {
            return new Subscriber<Integer>() {
                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                }

                @Override
                public void onNext(Integer integer) {
                    list.add(integer);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onComplete() {

                }
            };
        }

        public Subscription subscription() {
            return subscription;
        }

        public List<Integer> list() {
            return list;
        }

    }

}
