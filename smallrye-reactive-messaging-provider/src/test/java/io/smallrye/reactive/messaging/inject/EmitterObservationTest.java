package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.observation.DefaultMessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;
import io.smallrye.reactive.messaging.observation.ObservationContext;

public class EmitterObservationTest extends WeldTestBaseWithoutTails {

    @Test
    void testObservationPointsWhenEmittingPayloads() {
        addBeanClass(MyMessageObservationCollector.class);
        addBeanClass(MyComponentWithAnEmitterOfPayload.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyComponentWithAnEmitterOfPayload component = get(MyComponentWithAnEmitterOfPayload.class);

        component.emit(1);
        component.emit(2);
        component.emit(3);

        await().untilAsserted(() -> assertThat(observation.getObservations()).hasSize(3));
        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.isDone()).isTrue();
            assertThat(obs.getReason()).isNull();
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1);
        });

    }

    @Test
    void testObservationPointsWhenEmittingMessages() {
        addBeanClass(MyComponentWithAnEmitterOfMessage.class);
        addBeanClass(MyMessageObservationCollector.class);

        initialize();

        MyMessageObservationCollector observation = container.select(MyMessageObservationCollector.class).get();
        MyComponentWithAnEmitterOfMessage component = get(MyComponentWithAnEmitterOfMessage.class);

        component.emit(Message.of(1));
        component.emit(Message.of(2));
        component.emit(Message.of(3));

        await().untilAsserted(() -> assertThat(observation.getObservations()).hasSize(3));
        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.isDone()).isTrue();
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1);
        });

        assertThat(observation.getObservations().get(2).getReason()).isInstanceOf(IllegalArgumentException.class);

    }

    @ApplicationScoped
    public static class MyComponentWithAnEmitterOfPayload {

        @Inject
        @Channel("output")
        Emitter<Integer> emitter;

        public void emit(int i) {
            emitter.send(i);
        }

        @Incoming("output")
        public void consume(int i) {
            // do nothing.
        }

    }

    @ApplicationScoped
    public static class MyComponentWithAnEmitterOfMessage {

        @Inject
        @Channel("output")
        Emitter<Integer> emitter;

        public void emit(Message<Integer> i) {
            emitter.send(i);
        }

        @Incoming("output")
        public void consume(int i, MessageObservation mo) {
            assertThat(mo).isNotNull();
            if (i == 3) {
                throw new IllegalArgumentException("boom");
            }
        }

    }

    @ApplicationScoped
    public static class MyMessageObservationCollector implements MessageObservationCollector<ObservationContext> {

        private final List<MessageObservation> observations = new CopyOnWriteArrayList<>();

        public List<MessageObservation> getObservations() {
            return observations;
        }

        @Override
        public MessageObservation onNewMessage(String channel, Message<?> message, ObservationContext ctx) {
            MessageObservation observation = new DefaultMessageObservation(channel);
            observations.add(observation);
            return observation;
        }
    }
}
