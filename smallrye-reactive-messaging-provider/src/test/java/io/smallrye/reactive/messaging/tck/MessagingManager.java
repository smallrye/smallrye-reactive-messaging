package io.smallrye.reactive.messaging.tck;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.tck.incoming.Bean;

@ApplicationScoped
public class MessagingManager implements ChannelRegistar {

    private static final Message<MockPayload> DUMB = Message.of(new MockPayload());

    private Map<String, Source> topics = new HashMap<>();

    @Inject
    private ChannelRegistry registry;

    private final Map<String, MockedReceiver<?>> receivers = new ConcurrentHashMap<>();
    private final Map<String, MockedSender<?>> senders = new ConcurrentHashMap<>();

    public <T> MockedReceiver<T> getReceiver(String topic) {
        return (MockedReceiver<T>) receivers.computeIfAbsent(topic, t -> new MockedReceiver<T>(Duration.ofMillis(1000), topic));
    }

    public <T> MockedSender<T> getSender(String topic) {
        return (MockedSender<T>) senders.computeIfAbsent(topic, t -> new MockedSender<T>());
    }

    public Collection<String> listTopics() {
        return topics.keySet();
    }

    public void createTopics(List<String> newTopics) {
        newTopics.forEach(name -> {
            Source source = new Source(name);
            topics.put(name, source);
            this.registry.register(name, ReactiveStreams.fromPublisher(source.source()));
        });
    }

    public void send(String topic, Message<MockPayload> message) {
        Source processor = topics.get(topic);
        Objects.requireNonNull(processor);
        Objects.requireNonNull(message);
        processor.send(message);
    }

    public Optional<Message<MockPayload>> getLast(String topic, Duration timeout) {
        CompletableFuture<Message<MockPayload>> future = new CompletableFuture<>();
        Source source = topics.get(topic);
        Objects.requireNonNull(source);
        source.source().firstElement().subscribe(
                future::complete,
                future::completeExceptionally,
                () -> future.complete(DUMB));

        try {
            return Optional.ofNullable(future.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (TimeoutException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw new CompletionException(e.getCause());
        }
    }

    public void sendPayloads(String topic, MockPayload... payloads) {
        Source source = topics.get(topic);
        for (MockPayload payload : payloads) {
            source.sendWithAcknowledgement(payload);
        }
    }

    @Override
    public void initialize() {
        createTopics(Arrays.asList(
                Bean.VOID_METHOD,
                Bean.NON_PARALLEL,
                Bean.ASYNC_FAILING,
                Bean.INCOMING_OUTGOING_WRAPPED,
                Bean.NON_VOID_METHOD,
                Bean.OUTGOING_WRAPPED,
                Bean.SYNC_FAILING,
                Bean.WRAPPED_MESSAGE));
    }

    private static class Source {

        public Source(String name) {
            this.name = name;
        }

        private String name;
        private List<Message<MockPayload>> inflights = new ArrayList<>();
        private PublishProcessor<Message<MockPayload>> processor = PublishProcessor.create();
        private Flowable<Message<MockPayload>> source = processor
                .doOnSubscribe((x) -> resend());

        private void resend() {
            new ArrayList<>(inflights).forEach(m -> sendWithAcknowledgement(m.getPayload()));
        }

        public void sendWithAcknowledgement(MockPayload payload) {
            AtomicReference<Message<MockPayload>> reference = new AtomicReference<>();
            Message<MockPayload> msg = Message.of(payload, () -> {
                inflights.remove(reference.get());
                return CompletableFuture.completedFuture(null);
            });
            reference.set(msg);
            send(msg);
        }

        public void send(Message<MockPayload> msg) {
            inflights.add(msg);
            if (processor != null) {
                processor.onNext(msg);
            }
        }

        public Flowable<Message<MockPayload>> source() {
            return source;
        }

    }
}
