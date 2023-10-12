package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.observation.DefaultMessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;
import io.smallrye.reactive.messaging.observation.ObservationContext;

public class ObservationTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void setupConfig() {
        installConfig("src/test/resources/config/observation.properties");
    }

    @Test
    void testMessageObservationPointsFromPayloadConsumer() {
        addBeanClass(MyMessageObservationCollector.class);
        addBeanClass(MyPayloadConsumer.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyPayloadConsumer consumer = container.select(MyPayloadConsumer.class).get();

        await().until(() -> observation.getObservations().size() == 3);
        await().until(() -> consumer.received().size() == 3);

        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
        });

        assertThat(observation.getObservations().get(0).getReason()).isNull();
        assertThat(observation.getObservations().get(1).getReason()).isInstanceOf(IOException.class);
        assertThat(observation.getObservations().get(2).getReason()).isInstanceOf(MalformedURLException.class);
    }

    @Test
    void testMessageObservationPointsFromMessageConsumer() {
        addBeanClass(MyMessageObservationCollector.class);
        addBeanClass(MyMessageConsumer.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyMessageConsumer consumer = container.select(MyMessageConsumer.class).get();

        await().until(() -> observation.getObservations().size() == 3);
        await().until(() -> consumer.received().size() == 3);

        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
        });

        assertThat(observation.getObservations().get(0).getReason()).isNull();
        assertThat(observation.getObservations().get(1).getReason()).isInstanceOf(IOException.class);
        assertThat(observation.getObservations().get(2).getReason()).isInstanceOf(MalformedURLException.class);
    }

    @Test
    void testMessageObservationPointsFromMessageProducer() {
        addBeanClass(MyMessageObservationCollector.class);
        addBeanClass(MyMessageProducer.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyMessageProducer producer = container.select(MyMessageProducer.class).get();

        await().until(() -> observation.getObservations().size() == 3);
        await().until(() -> producer.acked().size() == 3);

        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getReason()).isNull();
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
        });
    }

    @Test
    void testMessageObservationPointsFromPayloadProducer() {
        addBeanClass(MyMessageObservationCollector.class);
        addBeanClass(MyPayloadProducer.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyPayloadProducer producer = container.select(MyPayloadProducer.class).get();

        await().until(() -> observation.getObservations().size() == 3);

        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getReason()).isNull();
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
        });
    }

    @Test
    void testMessageObservationPointsFromPayloadEmitter() {
        addBeanClass(MyMessageObservationCollector.class);
        addBeanClass(MyEmitterProducer.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyEmitterProducer producer = container.select(MyEmitterProducer.class).get();

        producer.produce();

        await().until(() -> observation.getObservations().size() == 3);

        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getReason()).isNull();
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
        });
    }

    @Test
    void testMessageObservationPointsFromMessageEmitter() {
        addBeanClass(MyMessageObservationCollector.class);
        addBeanClass(MyEmitterMessageProducer.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyEmitterMessageProducer producer = container.select(MyEmitterMessageProducer.class).get();

        producer.produceMessages();

        await().until(() -> observation.getObservations().size() == 3);
        await().until(() -> producer.acked().size() == 3);

        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getReason()).isNull();
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1).isGreaterThan(obs.getCreationTime());
        });
    }

    @ApplicationScoped
    public static class MyPayloadConsumer {
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("A")
        void consume(int payload, MessageObservation tracking) throws IOException {
            received.add(payload);
            assertThat(tracking).isNotNull();
            if (payload == 3) {
                throw new IOException();
            }
            if (payload == 4) {
                throw new MalformedURLException();
            }
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MyMessageConsumer {

        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("A")
        CompletionStage<Void> consume(Message<Integer> msg) {
            int payload = msg.getPayload();
            received.add(payload);
            assertThat(msg.getMetadata(MessageObservation.class)).isNotEmpty();
            if (payload == 3) {
                return msg.nack(new IOException());
            }
            if (payload == 4) {
                return msg.nack(new MalformedURLException());
            }
            return msg.ack();
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class MyPayloadProducer {

        private final List<Integer> acked = new CopyOnWriteArrayList<>();

        @Outgoing("B")
        Multi<Integer> produce() {
            return Multi.createFrom().items(1, 2, 3);
        }

        public List<Integer> acked() {
            return acked;
        }
    }

    @ApplicationScoped
    public static class MyMessageProducer {

        private final List<Integer> acked = new CopyOnWriteArrayList<>();

        @Outgoing("B")
        Multi<Message<Integer>> produce() {
            return Multi.createFrom().items(1, 2, 3)
                    .map(i -> Message.of(i, () -> {
                        acked.add(i);
                        return CompletableFuture.completedFuture(null);
                    }));
        }

        public List<Integer> acked() {
            return acked;
        }
    }

    @ApplicationScoped
    public static class MyEmitterProducer {

        @Inject
        @Channel("B")
        Emitter<Integer> emitter;

        void produce() {
            emitter.send(1);
            emitter.send(2);
            emitter.send(3);
        }

    }

    @ApplicationScoped
    public static class MyEmitterMessageProducer {

        private final List<Integer> acked = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("B")
        Emitter<Integer> emitter;

        void produceMessages() {
            for (int i = 0; i < 3; i++) {
                int j = i;
                emitter.send(Message.of(j, () -> {
                    acked.add(j);
                    return CompletableFuture.completedFuture(null);
                }));
            }
        }

        public List<Integer> acked() {
            return acked;
        }
    }

    @ApplicationScoped
    public static class MyMessageObservationCollector implements MessageObservationCollector<ObservationContext> {

        private final List<MessageObservation> observations = new CopyOnWriteArrayList<>();

        @Override
        public MessageObservation onNewMessage(String channel, Message<?> message, ObservationContext ctx) {
            MessageObservation observation = new DefaultMessageObservation(channel);
            observations.add(observation);
            return observation;
        }

        public List<MessageObservation> getObservations() {
            return observations;
        }

    }
}
