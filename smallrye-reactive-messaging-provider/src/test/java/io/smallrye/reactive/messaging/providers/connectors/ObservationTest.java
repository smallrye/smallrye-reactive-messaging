package io.smallrye.reactive.messaging.providers.connectors;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.observation.Observable;
import io.smallrye.reactive.messaging.observation.ObservationMetadata;
import io.smallrye.reactive.messaging.observation.ReactiveMessagingObservation;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ObservationTest extends WeldTestBaseWithoutTails {

    @Test
    void testThatConnectorImplementingObservableReceiveTheReactiveMessagingObservationInstance() {
        addBeanClass(MyConnector.class);
        addBeanClass(MyReactiveMessagingObservation.class);

        initialize();

        MyConnector connector = container.select(MyConnector.class, ConnectorLiteral.of("test-observation")).get();
        MyReactiveMessagingObservation observation = container.select(MyReactiveMessagingObservation.class).get();
        assertThat(connector.observation()).isNotNull();
        assertThat(connector.observation()).isInstanceOf(MyReactiveMessagingObservation.class);
        assertThat(connector.observation()).isSameAs(observation);
    }

    @Test
    void testMessageObservationPoints() {
        addBeanClass(MyConnector.class);
        addBeanClass(MyReactiveMessagingObservation.class);

        initialize();

        MyConnector connector = container.select(MyConnector.class, ConnectorLiteral.of("test-observation")).get();
        MyReactiveMessagingObservation observation = container.select(MyReactiveMessagingObservation.class).get();

        Multi<? extends Message<?>> stream = Multi.createFrom().publisher(connector.getPublisher(null));

        AssertSubscriber<? extends Message<?>> subscriber = stream.subscribe().withSubscriber(AssertSubscriber.create());

        subscriber.request(1);

        subscriber.awaitItems(1);
        await().until(() -> observation.getObservations().size() == 1);
        assertThat(observation.getObservations().get(0).getCreationTime()).isNotEqualTo(-1);
        assertThat(observation.getObservations().get(0).getProcessingTime()).isNotEqualTo(-1);
        assertThat(observation.getObservations().get(0).getCompletionTime()).isEqualTo(-1);

        subscriber.getItems().get(0).ack();
        await().untilAsserted(() -> assertThat(observation.getObservations().get(0).getCompletionTime()).isNotEqualTo(-1));


        subscriber.request(10);
        subscriber.awaitCompletion();

        await().until(() -> observation.getObservations().size() == 10);
        subscriber.getItems().get(1).nack(new IOException("boom"));
        subscriber.getItems().get(2).nack(new MalformedURLException("boom"));

        for (int i = 3; i < subscriber.getItems().size(); i++) {
            subscriber.getItems().get(i).ack();
        }

        assertThat(observation.getObservations()).allSatisfy(obs -> {
            assertThat(obs.getCreationTime()).isNotEqualTo(-1);
            assertThat(obs.getProcessingTime()).isNotEqualTo(-1);
            assertThat(obs.getCompletionTime()).isNotEqualTo(-1);
        });

        assertThat(observation.getObservations().get(1).getReason())
                .isInstanceOf(IOException.class);
        assertThat(observation.getObservations().get(2).getReason())
                .isInstanceOf(MalformedURLException.class);
    }

    @Connector("test-observation")
    @ApplicationScoped
    public static class MyConnector implements InboundConnector, Observable {

        private ReactiveMessagingObservation observation;

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().range(0, 10)
                    .map(Message::of)
                    .map(m -> {
                        ReactiveMessagingObservation.MessageObservation mo = observation.onNewMessage("ignored", m);
                        return m
                                .addMetadata(new ObservationMetadata(mo))
                                .withAck(() -> {
                                    mo.onAck();
                                    return m.ack();
                                }).withNack(t -> {
                                    mo.onNack(t);
                                    return m.nack(t);
                                });
                    })
                    .invoke(m -> m.getMetadata(ObservationMetadata.class).orElseThrow().observation().onProcessingStart());
        }

        @Override
        public void setReactiveMessageObservation(ReactiveMessagingObservation observation) {
            this.observation = observation;
        }

        public ReactiveMessagingObservation observation() {
            return observation;
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
