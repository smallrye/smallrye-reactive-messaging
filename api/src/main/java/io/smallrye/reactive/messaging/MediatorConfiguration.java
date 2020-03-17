package io.smallrye.reactive.messaging;

import java.lang.reflect.Method;
import java.util.List;

import javax.enterprise.inject.spi.Bean;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;

import io.smallrye.reactive.messaging.annotations.Merge;

public interface MediatorConfiguration {

    Shape shape();

    String getOutgoing();

    List<String> getIncoming();

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

    /**
     * Implementation of the {@link Invoker} interface that can be used to invoke the method described by this configuration
     * The invoker class can either have a no-arg constructor in which case it's expected to be look up the bean
     * programmatically, or have a constructor that takes a single Object parameter - the bean to operate on
     */
    Class<? extends Invoker> getInvokerClass();

    enum Production {
        STREAM_OF_MESSAGE,
        STREAM_OF_PAYLOAD,

        INDIVIDUAL_PAYLOAD,
        INDIVIDUAL_MESSAGE,
        COMPLETION_STAGE_OF_PAYLOAD,
        COMPLETION_STAGE_OF_MESSAGE,
        UNI_OF_PAYLOAD,
        UNI_OF_MESSAGE,

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
