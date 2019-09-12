package io.smallrye.reactive.messaging;

import java.lang.reflect.Method;

import javax.enterprise.inject.spi.Bean;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;

import io.smallrye.reactive.messaging.annotations.Merge;

public interface MediatorConfiguration {

    Shape shape();

    String getOutgoing();

    String getIncoming();

    String methodAsString();

    Method getMethod();

    Class<?> getReturnType();

    Class<?>[] getParameterTypes();

    Consumption consumption();

    Production production();

    boolean usesBuilderTypes();

    Acknowledgment.Strategy getAcknowledgment();

    Merge.Mode getMerge();

    boolean getBroadcast();

    Bean<?> getBean();

    int getNumberOfSubscriberBeforeConnecting();

    Class<? extends Invoker> getInvokerClass();

    enum Production {
        STREAM_OF_MESSAGE,
        STREAM_OF_PAYLOAD,

        INDIVIDUAL_PAYLOAD,
        INDIVIDUAL_MESSAGE,
        COMPLETION_STAGE_OF_PAYLOAD,
        COMPLETION_STAGE_OF_MESSAGE,

        NONE
    }

    enum Consumption {
        STREAM_OF_MESSAGE,
        STREAM_OF_PAYLOAD,

        MESSAGE,
        PAYLOAD,

        NONE
    }
}
