package io.smallrye.reactive.messaging.providers.connectors;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.connector.OutboundConnector;

@ApplicationScoped
@Connector("smallrye-slow")
public class SlowOutboundConnector implements OutboundConnector {

    private final List<String> received = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        int delayMs = config.getOptionalValue("delay-ms", Integer.class).orElse(200);
        return new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(Message<?> message) {
                executor.submit(() -> {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    received.add(message.getPayload().toString());
                    message.ack().toCompletableFuture().join();
                    subscription.request(1);
                });
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        };
    }

    public List<String> list() {
        return received;
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
