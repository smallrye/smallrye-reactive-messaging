package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.junit.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.beans.*;

public class SubscriberShapeTest extends WeldTestBaseWithoutTails {

    @Override
    public List<Class<?>> getBeans() {
        return Collections.singletonList(SourceOnly.class);
    }

    @Test
    public void testBeanProducingASubscriberOfMessages() {
        initializer.addBeanClasses(BeanReturningASubscriberOfMessages.class);
        initialize();
        BeanReturningASubscriberOfMessages collector = container.select(BeanReturningASubscriberOfMessages.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingASubscriberOfPayloads() {
        initializer.addBeanClasses(BeanReturningASubscriberOfPayloads.class);
        initialize();
        BeanReturningASubscriberOfPayloads collector = container.select(BeanReturningASubscriberOfPayloads.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testThatWeCanProduceSubscriberOfMessage() {
        initializer.addBeanClasses(BeanReturningASubscriberOfMessagesButDiscarding.class);
        initialize();
        assertThatSubscriberWasPublished(container);
    }

    @Test
    public void testThatWeCanConsumeMessagesFromAMethodReturningVoid() {
        // This case is not supported as it forces blocking acknowledgment.
        // See the MediatorConfiguration class for details.
        initializer.addBeanClasses(BeanConsumingMessagesAndReturningVoid.class);
        try {
            initialize();
            fail("Expected failure - method validation should have failed");
        } catch (DeploymentException e) {
            // Check we have the right cause
            assertThat(e).hasMessageContaining("Invalid method").hasMessageContaining("acknowledgment");
        }
    }

    @Test
    public void testThatWeCanConsumePayloadsFromAMethodReturningVoid() {
        initializer.addBeanClasses(BeanConsumingPayloadsAndReturningVoid.class);
        initialize();
        BeanConsumingPayloadsAndReturningVoid collector = container.getBeanManager()
                .createInstance().select(BeanConsumingPayloadsAndReturningVoid.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testThatWeCanConsumeMessagesFromAMethodReturningSomething() {
        // This case is not supported as it forces blocking acknowledgment.
        // See the MediatorConfiguration class for details.

        initializer.addBeanClasses(BeanConsumingMessagesAndReturningSomething.class);
        try {
            initialize();
            fail("Expected failure - method validation should have failed");
        } catch (DeploymentException e) {
            // Check we have the right cause
            assertThat(e).hasMessageContaining("Invalid method").hasMessageContaining("acknowledgment");
        }
    }

    @Test
    public void testThatWeCanConsumePayloadsFromAMethodReturningSomething() {
        initializer.addBeanClasses(BeanConsumingPayloadsAndReturningSomething.class);
        initialize();
        BeanConsumingPayloadsAndReturningSomething collector = container.getBeanManager()
                .createInstance().select(BeanConsumingPayloadsAndReturningSomething.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testThatWeCanConsumeMessagesFromAMethodReturningACompletionStage() {
        initializer.addBeanClasses(BeanConsumingMessagesAndReturningACompletionStageOfVoid.class);
        initialize();
        BeanConsumingMessagesAndReturningACompletionStageOfVoid collector = container.getBeanManager()
                .createInstance().select(BeanConsumingMessagesAndReturningACompletionStageOfVoid.class).get();
        await().until(() -> collector.payloads().size() == EXPECTED.size());
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
        collector.close();
    }

    @Test
    public void testThatWeCanConsumePayloadsFromAMethodReturningACompletionStage() {
        initializer.addBeanClasses(BeanConsumingPayloadsAndReturningACompletionStageOfVoid.class);
        initialize();
        BeanConsumingPayloadsAndReturningACompletionStageOfVoid collector = container.getBeanManager()
                .createInstance().select(BeanConsumingPayloadsAndReturningACompletionStageOfVoid.class).get();
        await().until(() -> collector.payloads().size() == EXPECTED.size());
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
        collector.close();
    }

    @Test
    public void testThatWeCanConsumeMessagesFromAMethodReturningACompletionStageOfSomething() {
        initializer.addBeanClasses(BeanConsumingMessagesAndReturningACompletionStageOfSomething.class);
        initialize();
        BeanConsumingMessagesAndReturningACompletionStageOfSomething collector = container.getBeanManager()
                .createInstance().select(BeanConsumingMessagesAndReturningACompletionStageOfSomething.class).get();
        await().until(() -> collector.payloads().size() == EXPECTED.size());
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
        collector.close();
    }

    @Test
    public void testThatWeCanConsumePayloadsFromAMethodReturningACompletionStageOfSomething() {
        initializer.addBeanClasses(BeanConsumingPayloadsAndReturningACompletionStageOfSomething.class);
        initialize();
        BeanConsumingPayloadsAndReturningACompletionStageOfSomething collector = container.getBeanManager()
                .createInstance().select(BeanConsumingPayloadsAndReturningACompletionStageOfSomething.class).get();
        await().until(() -> collector.payloads().size() == EXPECTED.size());
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
        collector.close();
    }

    @SuppressWarnings("unchecked")
    private void assertThatSubscriberWasPublished(SeContainer container) {
        assertThat(registry(container).getOutgoingNames()).contains("subscriber");
        List<SubscriberBuilder<? extends Message, Void>> subscriber = registry(container).getSubscribers("subscriber");
        assertThat(subscriber).isNotEmpty();
        List<String> list = new ArrayList<>();
        Multi.createFrom().items("a", "b", "c").map(Message::of)
                .onItem().invoke(m -> list.add(m.getPayload()))
                .subscribe(((SubscriberBuilder) subscriber.get(0)).build());
        assertThat(list).containsExactly("a", "b", "c");
    }

}
