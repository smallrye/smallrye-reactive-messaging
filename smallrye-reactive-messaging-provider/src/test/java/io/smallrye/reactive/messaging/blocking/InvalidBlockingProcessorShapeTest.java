package io.smallrye.reactive.messaging.blocking;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class InvalidBlockingProcessorShapeTest extends WeldTestBaseWithoutTails {
    @Test(expected = DeploymentException.class)
    public void testBeanProducingACompletionStageOfMessage() {
        addBeanClass(BeanProducingACompletionStageOfMessage.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingACompletionStageOfPayloads() {
        addBeanClass(BeanProducingACompletionStage.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingACompletableFutureOfMessage() {
        addBeanClass(BeanProducingACompletableFutureOfMessage.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingACompletableFutureOfPayloads() {
        addBeanClass(BeanProducingACompletableFuture.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAProcessorOfMessages() {
        addBeanClass(BeanProducingAProcessorOfMessages.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAProcessorBuilderOfMessages() {
        addBeanClass(BeanProducingAProcessorBuilderOfMessages.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAProcessorOfPayloads() {
        addBeanClass(BeanProducingAProcessorOfPayloads.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAProcessorBuilderOfPayloads() {
        addBeanClass(BeanProducingAProcessorBuilderOfPayloads.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAPublisherOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAPublisherOfMessagesAndConsumingIndividualMessage.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAPublisherOfPayloadsAndConsumingIndividualPayload.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload() {
        addBeanClass(BeanProducingAPublisherBuilderOfPayloadsAndConsumingIndividualPayload.class);
        initialize();
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

    @Test(expected = DeploymentException.class)
    public void testBeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage() {
        addBeanClass(BeanProducingAPublisherBuilderOfMessagesAndConsumingIndividualMessage.class);
        initialize();
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
}
