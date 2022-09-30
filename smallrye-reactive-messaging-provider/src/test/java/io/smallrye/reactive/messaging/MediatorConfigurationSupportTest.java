package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.inject.spi.DefinitionException;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.providers.DefaultMediatorConfiguration;
import io.smallrye.reactive.messaging.providers.MediatorConfigurationSupport;

@SuppressWarnings("ConstantConditions")
public class MediatorConfigurationSupportTest {

    static Class<ClassContainingAllSortsOfMethods> clazz = ClassContainingAllSortsOfMethods.class;

    private MediatorConfigurationSupport create(String method) {
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.getName().equalsIgnoreCase(method)) {
                return new MediatorConfigurationSupport(
                        method,
                        m.getReturnType(),
                        m.getParameterTypes(),
                        new DefaultMediatorConfiguration.ReturnTypeGenericTypeAssignable(m),
                        m.getParameterTypes().length == 0
                                ? new DefaultMediatorConfiguration.AlwaysInvalidIndexGenericTypeAssignable()
                                : new DefaultMediatorConfiguration.MethodParamGenericTypeAssignable(m, 0));
            }
        }
        fail("Unable to find method " + method);
        return null;
    }

    @Test
    public void testPublishers() {
        MediatorConfigurationSupport support = create("publisherPublisherOfMessage");
        MediatorConfigurationSupport.ValidationOutput output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherMultiOfMessage");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherPublisherOfPayload");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherMultiOfPayload");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherPublisherBuilderOfMessage");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("publisherPublisherBuilderOfPayload");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("publisherGeneratePayload");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherGenerateMessage");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.INDIVIDUAL_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherGenerateCompletionStagePayload");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.COMPLETION_STAGE_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherGenerateCompletionStageMessage");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.COMPLETION_STAGE_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherGenerateUniPayload");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.UNI_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("publisherGenerateUniMessage");
        output = support.validate(Shape.PUBLISHER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.NONE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.UNI_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isNull();
        assertThat(output.getUseBuilderTypes()).isFalse();
    }

    @Test
    public void testSubscribers() {
        MediatorConfigurationSupport support = create("subscriberSubscriberOfMessage");
        MediatorConfigurationSupport.ValidationOutput output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSubscriberOfPayload");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSubscriberBuilderOfMessage");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("subscriberSubscriberBuilderOfPayload");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        assertThatThrownBy(() -> create("subscriberSinkOfMessage").validate(Shape.SUBSCRIBER, null))
                .isInstanceOf(DefinitionException.class);

        support = create("subscriberSinkOfPayload");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSinkOfMessageCompletionStage");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSinkOfPayloadCompletionStage");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSinkOfMessageUni");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSinkOfPayloadUni");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSinkOfRawMessageCompletionStage");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(null);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("subscriberSinkOfWildcardMessageCompletionStage");
        output = support.validate(Shape.SUBSCRIBER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.NONE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(null);
        assertThat(output.getUseBuilderTypes()).isFalse();
    }

    @Test
    public void testProcessors() {
        MediatorConfigurationSupport support = create("processorProcessorOfMessage");
        MediatorConfigurationSupport.ValidationOutput output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessorOfPayload");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessorBuilderOfMessage");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("processorProcessorBuilderOfPayload");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("processorPublisherOfMessage");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorPublisherOfPayload");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorPublisherBuilderOfMessage");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("processorPublisherBuilderOfPayload");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(String.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("processorProcessMessage");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.INDIVIDUAL_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessPayload");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessMessageCompletionStage");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.COMPLETION_STAGE_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessPayloadCompletionStage");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.COMPLETION_STAGE_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessMessageUni");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.UNI_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessPayloadUni");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.UNI_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessMessageUniRaw");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.UNI_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(null);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("processorProcessMessageUniWildcard");
        output = support.validate(Shape.PROCESSOR, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.UNI_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(null);
        assertThat(output.getUseBuilderTypes()).isFalse();
    }

    @Test
    public void testStreamTransformers() {
        MediatorConfigurationSupport support = create("transformerPublisherOfMessage");
        MediatorConfigurationSupport.ValidationOutput output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("transformerPublisherOfPayload");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("transformerMultiOfMessage");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("transformerMultiOfPayload");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("transformerPublisherBuilderOfMessage");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("transformerPublisherBuilderOfPayload");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(Person.class);
        assertThat(output.getUseBuilderTypes()).isTrue();

        support = create("transformerPublisherOfMessageRaw");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(null);
        assertThat(output.getUseBuilderTypes()).isFalse();

        support = create("transformerPublisherOfMessageWildcard");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_MESSAGE);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_MESSAGE);
        assertThat(output.getIngestedPayloadType()).isEqualTo(null);
        assertThat(output.getUseBuilderTypes()).isFalse();

        assertThatThrownBy(() -> create("transformerPublisherOfPayloadRaw").validate(Shape.STREAM_TRANSFORMER, null))
                .isInstanceOf(DefinitionException.class);

        support = create("transformerPublisherOfPayloadWildcard");
        output = support.validate(Shape.STREAM_TRANSFORMER, null);
        assertThat(output.getConsumption()).isEqualTo(MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD);
        assertThat(output.getProduction()).isEqualTo(MediatorConfiguration.Production.STREAM_OF_PAYLOAD);
        assertThat(output.getIngestedPayloadType()).isEqualTo(null);
        assertThat(output.getUseBuilderTypes()).isFalse();
    }

    @Test
    void testDetermineShape() {
        assertThat(new MediatorConfigurationSupport("mymethod", String.class, new Class[] { String.class }, null, null)
                .determineShape(Collections.singletonList("incoming"), "outgoing")).isEqualTo(Shape.PROCESSOR);

        assertThat(
                new MediatorConfigurationSupport("mymethod", Multi.class, new Class[] { Publisher.class }, null, null)
                        .determineShape(Collections.singletonList("incoming"), "outgoing")).isEqualTo(Shape.STREAM_TRANSFORMER);

        assertThat(
                new MediatorConfigurationSupport("mymethod", PublisherBuilder.class, new Class[] { PublisherBuilder.class },
                        null, null)
                                .determineShape(Collections.singletonList("incoming"), "outgoing"))
                                        .isEqualTo(Shape.STREAM_TRANSFORMER);

        assertThat(
                new MediatorConfigurationSupport("mymethod", PublisherBuilder.class, new Class[] { String.class }, null,
                        null)
                                .determineShape(Collections.singletonList("incoming"), "outgoing")).isEqualTo(Shape.PROCESSOR);

        assertThat(new MediatorConfigurationSupport("mymethod", String.class, new Class[0], null, null)
                .determineShape(Collections.emptyList(), "outgoing")).isEqualTo(Shape.PUBLISHER);

        assertThat(new MediatorConfigurationSupport("mymethod", Void.class, new Class[] { String.class }, null, null)
                .determineShape(Collections.singletonList("incoming"), null)).isEqualTo(Shape.SUBSCRIBER);
    }

    @Test
    void testProcessSuppliedAcknowledgement() {
        MediatorConfigurationSupport support = new MediatorConfigurationSupport("mymethod", String.class,
                new Class[] { String.class }, null, null);

        assertThat(support
                .processSuppliedAcknowledgement(Collections.singletonList("hello"), () -> Acknowledgment.Strategy.MANUAL))
                        .isEqualTo(Acknowledgment.Strategy.MANUAL);

        assertThat(support.processSuppliedAcknowledgement(Collections.singletonList("hello"), () -> null))
                .isNull();

        assertThat(support.processSuppliedAcknowledgement(Collections.emptyList(), () -> null))
                .isNull();

        assertThatThrownBy(
                () -> support.processSuppliedAcknowledgement(Collections.emptyList(), () -> Acknowledgment.Strategy.MANUAL))
                        .isInstanceOf(DefinitionException.class);
    }

    @Test
    void testProcessDefaultAcknowledgement() {
        MediatorConfigurationSupport support = new MediatorConfigurationSupport("mymethod", String.class,
                new Class[] { String.class }, null, null);

        assertThat(support.processDefaultAcknowledgement(Shape.SUBSCRIBER, MediatorConfiguration.Consumption.PAYLOAD,
                MediatorConfiguration.Production.NONE))
                        .isEqualTo(Acknowledgment.Strategy.POST_PROCESSING);
        assertThat(support.processDefaultAcknowledgement(Shape.SUBSCRIBER, MediatorConfiguration.Consumption.MESSAGE,
                MediatorConfiguration.Production.NONE))
                        .isEqualTo(Acknowledgment.Strategy.MANUAL);

        assertThat(support.processDefaultAcknowledgement(Shape.PROCESSOR, MediatorConfiguration.Consumption.PAYLOAD,
                MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD))
                        .isEqualTo(Acknowledgment.Strategy.POST_PROCESSING);
        assertThat(support.processDefaultAcknowledgement(Shape.PROCESSOR, MediatorConfiguration.Consumption.MESSAGE,
                MediatorConfiguration.Production.INDIVIDUAL_MESSAGE))
                        .isEqualTo(Acknowledgment.Strategy.MANUAL);
        assertThat(support.processDefaultAcknowledgement(Shape.PROCESSOR, MediatorConfiguration.Consumption.PAYLOAD,
                MediatorConfiguration.Production.STREAM_OF_MESSAGE))
                        .isEqualTo(Acknowledgment.Strategy.PRE_PROCESSING);
        assertThat(support.processDefaultAcknowledgement(Shape.PROCESSOR, MediatorConfiguration.Consumption.PAYLOAD,
                MediatorConfiguration.Production.STREAM_OF_PAYLOAD))
                        .isEqualTo(Acknowledgment.Strategy.PRE_PROCESSING);

        assertThat(support.processDefaultAcknowledgement(Shape.STREAM_TRANSFORMER,
                MediatorConfiguration.Consumption.STREAM_OF_MESSAGE, MediatorConfiguration.Production.STREAM_OF_MESSAGE))
                        .isEqualTo(Acknowledgment.Strategy.MANUAL);
        assertThat(support.processDefaultAcknowledgement(Shape.STREAM_TRANSFORMER,
                MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD, MediatorConfiguration.Production.STREAM_OF_PAYLOAD))
                        .isEqualTo(Acknowledgment.Strategy.PRE_PROCESSING);
        assertThat(support.processDefaultAcknowledgement(Shape.STREAM_TRANSFORMER,
                MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD, MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD))
                        .isEqualTo(Acknowledgment.Strategy.PRE_PROCESSING);
    }

    @Test
    void testProcessMerge() {
        MediatorConfigurationSupport support = new MediatorConfigurationSupport("mymethod", String.class,
                new Class[] { String.class }, null, null);

        assertThat(support.processMerge(Arrays.asList("a", "b"), () -> Merge.Mode.MERGE)).isEqualTo(Merge.Mode.MERGE);

        assertThatThrownBy(() -> support.processMerge(Collections.emptyList(), () -> Merge.Mode.MERGE))
                .isInstanceOf(DefinitionException.class);
        assertThatThrownBy(() -> support.processMerge(null, () -> Merge.Mode.MERGE))
                .isInstanceOf(DefinitionException.class);
        assertThat(support.processMerge(Collections.emptyList(), () -> null)).isEqualTo(null);
    }

    @Test
    void testProcessBroadcast() {
        MediatorConfigurationSupport support = new MediatorConfigurationSupport("mymethod", String.class,
                new Class[] { String.class }, null, null);
        assertThat(support.processBroadcast("hello", () -> 1)).isEqualTo(1);
        assertThatThrownBy(() -> support.processBroadcast(null, () -> 1)).isInstanceOf(DefinitionException.class);

        assertThat(support.processBroadcast(null, () -> null)).isNull();
    }

    @Test
    void testValidateBlocking() {
        MediatorConfigurationSupport support = new MediatorConfigurationSupport("mymethod", String.class,
                new Class[] { String.class }, null, null);

        support.validateBlocking(
                new MediatorConfigurationSupport.ValidationOutput(MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD,
                        MediatorConfiguration.Consumption.PAYLOAD, String.class));

        assertThatThrownBy(() -> support.validateBlocking(
                new MediatorConfigurationSupport.ValidationOutput(MediatorConfiguration.Production.STREAM_OF_PAYLOAD,
                        MediatorConfiguration.Consumption.PAYLOAD, String.class))).isInstanceOf(DefinitionException.class);

        assertThatThrownBy(() -> support.validateBlocking(
                new MediatorConfigurationSupport.ValidationOutput(MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD,
                        MediatorConfiguration.Consumption.STREAM_OF_MESSAGE, String.class)))
                                .isInstanceOf(DefinitionException.class);

    }

    static class ClassContainingAllSortsOfMethods {

        // Publishers
        Publisher<Message<Person>> publisherPublisherOfMessage() {
            return null;
        }

        Multi<Message<Person>> publisherMultiOfMessage() {
            return null;
        }

        Publisher<Person> publisherPublisherOfPayload() {
            return null;
        }

        Multi<Person> publisherMultiOfPayload() {
            return null;
        }

        PublisherBuilder<Message<Person>> publisherPublisherBuilderOfMessage() {
            return null;
        }

        PublisherBuilder<Person> publisherPublisherBuilderOfPayload() {
            return null;
        }

        Person publisherGeneratePayload() {
            return null;
        }

        Message<Person> publisherGenerateMessage() {
            return null;
        }

        CompletionStage<Person> publisherGenerateCompletionStagePayload() {
            return null;
        }

        CompletionStage<Message<Person>> publisherGenerateCompletionStageMessage() {
            return null;
        }

        Uni<Person> publisherGenerateUniPayload() {
            return null;
        }

        Uni<Message<Person>> publisherGenerateUniMessage() {
            return null;
        }

        // Subscribers
        Subscriber<Message<Person>> subscriberSubscriberOfMessage() {
            return null;
        }

        Subscriber<Person> subscriberSubscriberOfPayload() {
            return null;
        }

        SubscriberBuilder<Message<Person>, Void> subscriberSubscriberBuilderOfMessage() {
            return null;
        }

        SubscriberBuilder<Person, Void> subscriberSubscriberBuilderOfPayload() {
            return null;
        }

        void subscriberSinkOfMessage(Message<Person> p) {
            // Invalid
        }

        void subscriberSinkOfPayload(Person p) {

        }

        CompletionStage<Void> subscriberSinkOfMessageCompletionStage(Message<Person> p) {
            return null;
        }

        CompletionStage<Void> subscriberSinkOfPayloadCompletionStage(Person p) {
            return null;
        }

        Uni<Void> subscriberSinkOfMessageUni(Message<Person> p) {
            return null;
        }

        Uni<Void> subscriberSinkOfPayloadUni(Person p) {
            return null;
        }

        @SuppressWarnings("rawtypes")
        CompletionStage<Void> subscriberSinkOfRawMessageCompletionStage(Message p) {
            return null;
        }

        CompletionStage<Void> subscriberSinkOfWildcardMessageCompletionStage(Message<?> p) {
            return null;
        }

        // Processors

        Processor<Message<String>, Message<Person>> processorProcessorOfMessage() {
            return null;
        }

        Processor<String, Person> processorProcessorOfPayload() {
            return null;
        }

        ProcessorBuilder<Message<String>, Message<Person>> processorProcessorBuilderOfMessage() {
            return null;
        }

        ProcessorBuilder<String, Person> processorProcessorBuilderOfPayload() {
            return null;
        }

        Publisher<Message<Person>> processorPublisherOfMessage(Message<String> in) {
            return null;
        }

        Publisher<Person> processorPublisherOfPayload(String in) {
            return null;
        }

        PublisherBuilder<Message<Person>> processorPublisherBuilderOfMessage(Message<String> in) {
            return null;
        }

        PublisherBuilder<Person> processorPublisherBuilderOfPayload(String in) {
            return null;
        }

        Message<String> processorProcessMessage(Message<Person> in) {
            return null;
        }

        String processorProcessPayload(Person in) {
            return null;
        }

        CompletionStage<Message<String>> processorProcessMessageCompletionStage(Message<Person> in) {
            return null;
        }

        CompletionStage<String> processorProcessPayloadCompletionStage(Person in) {
            return null;
        }

        Uni<Message<String>> processorProcessMessageUni(Message<Person> in) {
            return null;
        }

        Uni<String> processorProcessPayloadUni(Person in) {
            return null;
        }

        Uni<Message<String>> processorProcessMessageUniRaw(Message in) {
            return null;
        }

        Uni<Message<String>> processorProcessMessageUniWildcard(Message<?> in) {
            return null;
        }

        // Transformers

        Publisher<Message<String>> transformerPublisherOfMessage(Publisher<Message<Person>> in) {
            return null;
        }

        Multi<Message<String>> transformerMultiOfMessage(Multi<Message<Person>> in) {
            return null;
        }

        PublisherBuilder<Message<String>> transformerPublisherBuilderOfMessage(PublisherBuilder<Message<Person>> in) {
            return null;
        }

        Publisher<String> transformerPublisherOfPayload(Publisher<Person> in) {
            return null;
        }

        Multi<String> transformerMultiOfPayload(Multi<Person> in) {
            return null;
        }

        PublisherBuilder<String> transformerPublisherBuilderOfPayload(PublisherBuilder<Person> in) {
            return null;
        }

        @SuppressWarnings("rawtypes")
        Publisher<Message<String>> transformerPublisherOfMessageRaw(Publisher<Message> in) {
            return null;
        }

        Publisher<Message<String>> transformerPublisherOfMessageWildcard(Publisher<Message<?>> in) {
            return null;
        }

        @SuppressWarnings("rawtypes")
        Publisher<String> transformerPublisherOfPayloadRaw(Publisher in) {
            // invalid
            return null;
        }

        Publisher<String> transformerPublisherOfPayloadWildcard(Publisher<?> in) {
            return null;
        }

    }

    static class Person {

    }

}
