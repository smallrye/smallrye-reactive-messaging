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
import io.smallrye.reactive.messaging.observation.ObservationMetadata;
import io.smallrye.reactive.messaging.observation.ReactiveMessagingObservation;

public class EmitterObservationTest extends WeldTestBaseWithoutTails {

    @Test
    void testObservationPointsWhenEmittingPayloads() {
        addBeanClass(MyComponentWithAnEmitterOfPayload.class);
        addBeanClass(MyReactiveMessagingObservation.class);

        initialize();

        MyReactiveMessagingObservation observation = container.select(MyReactiveMessagingObservation.class).get();
        MyComponentWithAnEmitterOfPayload component = get(MyComponentWithAnEmitterOfPayload.class);

        component.emit(1);
        component.emit(2);
        component.emit(3);

        await().until(() -> observation.getObservations().size() == 3);
        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getProcessingTime()).isNotEqualTo(-1);
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1);
        });

    }

    @Test
    void testObservationPointsWhenEmittingMessages() {
        addBeanClass(MyComponentWithAnEmitterOfMessage.class);
        addBeanClass(MyReactiveMessagingObservation.class);

        initialize();

        MyReactiveMessagingObservation observation = container.select(MyReactiveMessagingObservation.class).get();
        MyComponentWithAnEmitterOfMessage component = get(MyComponentWithAnEmitterOfMessage.class);

        component.emit(Message.of(1));
        component.emit(Message.of(2));
        component.emit(Message.of(3));

        await().until(() -> observation.getObservations().size() == 3);
        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getProcessingTime()).isNotEqualTo(-1);
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
        public void consume(int i, ObservationMetadata mo) {
            assertThat(mo).isNotNull();
            if (i == 3) {
                throw new IllegalArgumentException("boom");
            }
        }

    }

    @ApplicationScoped
    public static class MyReactiveMessagingObservation implements ReactiveMessagingObservation {

        private List<MyMessageObservation> observations = new CopyOnWriteArrayList<>();

        @Override
        public MessageObservation onNewMessage(String channel, Message<?> message) {
            MyMessageObservation observation = new MyMessageObservation(channel, message);
            observations.add(observation);
            return observation;
        }

        public List<MyMessageObservation> getObservations() {
            return observations;
        }

        public static class MyMessageObservation extends MessageObservation {

            public MyMessageObservation(String channel, Message<?> message) {
                super(channel, message);
            }

            public long getCreationTime() {
                return creationTime;
            }

            public long getProcessingTime() {
                return processing;
            }

            public long getCompletionTime() {
                return completion;
            }

            public Throwable getReason() {
                return nackReason;
            }

        }
    }
}
