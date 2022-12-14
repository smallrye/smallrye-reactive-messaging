package io.smallrye.reactive.messaging.providers.extension;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

/**
 * Reproducer for https://github.com/smallrye/smallrye-reactive-messaging/issues/719.
 */
public class HealthWithSubscriberTest extends WeldTestBaseWithoutTails {

    @Test
    public void testHealthCheckWithSubscriber() {
        addBeanClass(MyBean.class);

        initialize();
        HealthCenter center = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        MyBean bean = container.getBeanManager().createInstance().select(MyBean.class).get();

        assertThat(center.getLiveness().isOk()).isTrue();
        assertThat(center.getLiveness().getChannels()).isEmpty();
        assertThat(center.getReadiness().isOk()).isTrue();
        assertThat(center.getReadiness().getChannels()).isEmpty();

        assertThat(bean.items()).containsExactly("a", "b", "c", "d", "e");
    }

    @SuppressWarnings("ReactiveStreamsSubscriberImplementation")
    @ApplicationScoped
    public static class MyBean {

        private final List<String> items = new ArrayList<>();

        public List<String> items() {
            return items;
        }

        @Outgoing("sink")
        public Multi<String> source() {
            return Multi.createFrom().items("a", "b", "c", "d", "e");
        }

        @Incoming("sink")
        public Subscriber<String> getSubscriber() {
            return new Subscriber<String>() {

                Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(String s) {
                    items.add(s);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable t) {
                    // ignored
                }

                @Override
                public void onComplete() {
                    // ignored
                }
            };
        }

    }
}
