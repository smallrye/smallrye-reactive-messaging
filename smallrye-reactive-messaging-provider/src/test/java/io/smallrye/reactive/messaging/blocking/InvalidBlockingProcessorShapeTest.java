package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class InvalidBlockingProcessorShapeTest extends WeldTestBaseWithoutTails {
    @Test
    public void testBeanProducingUniOfPayloads() {
        addBeanClass(BeanProducingUni.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingUni {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Uni<String> process(int value) {
            return Uni.createFrom().item(() -> Integer.toString(value + 1));
        }
    }

    @Test
    public void testBeanProducingUniMessage() {
        addBeanClass(BeanProducingUniOfMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingUniOfMessage {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Uni<Message<String>> process(Message<Integer> value) {
            return Uni.createFrom().item(() -> Integer.toString(value.getPayload() + 1)).map(Message::of);
        }
    }

    @Test
    public void testBeanProducingACompletionStageOfMessage() {
        addBeanClass(BeanProducingACompletionStageOfMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingACompletionStageOfMessage {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public CompletionStage<Message<String>> process(Message<Integer> value) {
            return CompletableFuture.supplyAsync(() -> Integer.toString(value.getPayload() + 1))
                    .thenApply(Message::of);
        }
    }

    @Test
    public void testBeanProducingACompletionStageOfPayloads() {
        addBeanClass(BeanProducingACompletionStage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingACompletionStage {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public CompletionStage<String> process(int value) {
            return CompletableFuture.supplyAsync(() -> Integer.toString(value + 1));
        }
    }

    @Test
    public void testBeanProducingACompletableFutureOfMessage() {
        addBeanClass(BeanProducingACompletableFutureOfMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingACompletableFutureOfMessage {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public CompletionStage<Message<String>> process(Message<Integer> value) {
            return CompletableFuture.supplyAsync(() -> Integer.toString(value.getPayload() + 1))
                    .thenApply(Message::of);
        }
    }

    @Test
    public void testBeanProducingACompletableFutureOfPayloads() {
        addBeanClass(BeanProducingACompletableFuture.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingACompletableFuture {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public CompletableFuture<String> process(int value) {
            return CompletableFuture.supplyAsync(() -> Integer.toString(value + 1));
        }
    }

    @Test
    public void testBeanProducingAProcessorOfMessages() {
        addBeanClass(BeanProducingAProcessorOfMessages.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAProcessorOfMessages {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Processor<Message<Integer>, Message<String>> process() {
            return ReactiveStreams.<Message<Integer>> builder()
                    .map(Message::getPayload)
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i))
                    .map(Message::of)
                    .buildRs();
        }
    }

    @Test
    public void testBeanProducingAProcessorBuilderOfMessages() {
        addBeanClass(BeanProducingAProcessorBuilderOfMessages.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAProcessorBuilderOfMessages {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public ProcessorBuilder<Message<Integer>, Message<String>> process() {
            return ReactiveStreams.<Message<Integer>> builder()
                    .map(Message::getPayload)
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i))
                    .map(Message::of);
        }
    }

    @Test
    public void testBeanProducingAProcessorOfPayloads() {
        addBeanClass(BeanProducingAProcessorOfPayloads.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAProcessorOfPayloads {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Processor<Integer, String> process() {
            return ReactiveStreams.<Integer> builder()
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i))
                    .buildRs();
        }
    }

    @Test
    public void testBeanProducingAProcessorBuilderOfPayloads() {
        addBeanClass(BeanProducingAProcessorBuilderOfPayloads.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAProcessorBuilderOfPayloads {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public ProcessorBuilder<Integer, String> process() {
            return ReactiveStreams.<Integer> builder()
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i));
        }
    }

    @Test
    public void testBeanProducingAPublisherOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Publisher<Message<String>> process(Message<Integer> message) {
            return ReactiveStreams.of(message)
                    .map(Message::getPayload)
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i))
                    .map(Message::of)
                    .buildRs();
        }
    }

    @Test
    public void testBeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Publisher<String> process(Integer payload) {
            return ReactiveStreams.of(payload)
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i))
                    .buildRs();
        }
    }

    @Test
    public void testBeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public PublisherBuilder<String> process(Integer payload) {
            return ReactiveStreams.of(payload)
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i));
        }
    }

    @Test
    public void testBeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public PublisherBuilder<Message<String>> process(Message<Integer> message) {
            return ReactiveStreams.of(message)
                    .map(Message::getPayload)
                    .map(i -> i + 1)
                    .flatMapRsPublisher(i -> Flowable.just(i, i))
                    .map(i -> Integer.toString(i))
                    .map(Message::of);
        }
    }

    @Test
    public void testBeanProducingAMultiOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAMultiOfPayloadsAndConsumingIndividualPayload.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAMultiOfPayloadsAndConsumingIndividualPayload {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Multi<String> process(Integer payload) {
            return Multi.createFrom().publisher(
                    ReactiveStreams.of(payload)
                            .map(i -> i + 1)
                            .flatMapRsPublisher(i -> Flowable.just(i, i))
                            .map(i -> Integer.toString(i)).buildRs());
        }
    }

    @Test
    public void testBeanProducingAMultiOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAMultiOfMessagesAndConsumingIndividualMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class BeanProducingAMultiOfMessagesAndConsumingIndividualMessage {
        @Blocking
        @Incoming("count")
        @Outgoing("sink")
        public Multi<Message<String>> process(Message<Integer> message) {
            return Multi.createFrom().publisher(
                    ReactiveStreams.of(message)
                            .map(Message::getPayload)
                            .map(i -> i + 1)
                            .flatMapRsPublisher(i -> Flowable.just(i, i))
                            .map(i -> Integer.toString(i))
                            .map(Message::of).buildRs());
        }
    }
}
