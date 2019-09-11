package io.smallrye.reactive.messaging;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

import javax.enterprise.inject.spi.Bean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Merge;

public class DefaultMediatorConfiguration implements MediatorConfiguration {

    private final Bean<?> mediatorBean;

    private final Method method;

    private Class<?> returnType;

    private Class<?>[] parameterTypes;

    private Shape shape;

    private String incomingValue = null;

    private String outgoingValue = null;

    private Acknowledgment.Strategy acknowledgment;

    private Integer broadcastValue = null;

    /**
     * What does the mediator products and how is it produced
     */
    private Production production = Production.NONE;
    /**
     * What does the mediator consumes and how is it produced
     */
    private Consumption consumption = Consumption.NONE;

    /**
     * Use MicroProfile Stream Stream Ops Type.
     */
    private boolean useBuilderTypes = false;

    /**
     * The merge policy.
     */
    private Merge.Mode mergePolicy;

    private Class<? extends Invoker> invokerClass;

    private MediaConfigurationSupport mediaConfigurationSupport;

    public DefaultMediatorConfiguration(Method method, Bean<?> bean) {
        this.method = Objects.requireNonNull(method, "'method' must be set");
        this.returnType = method.getReturnType();
        this.parameterTypes = method.getParameterTypes();
        this.mediatorBean = Objects.requireNonNull(bean, "'bean' must be set");

        this.mediaConfigurationSupport = new MediaConfigurationSupport(methodAsString(), this.returnType, this.parameterTypes,
                new ReturnTypeGenericTypeAssignable(method),
                this.parameterTypes.length == 0 ? new AlwaysInvalidIndexGenericTypeAssignable()
                        : new MethodParamGenericTypeAssignable(method, 0));
    }

    public void compute(Incoming incoming, Outgoing outgoing) {

        if (incoming != null && StringUtils.isBlank(incoming.value())) {
            throw getIncomingError("value is blank or null");
        }

        if (outgoing != null && StringUtils.isBlank(outgoing.value())) {
            throw getOutgoingError("value is blank or null");
        }

        this.shape = this.mediaConfigurationSupport.determineShape(incoming, outgoing);

        this.acknowledgment = this.mediaConfigurationSupport.processSuppliedAcknowledgement(incoming, () -> {
            Acknowledgment annotation = method.getAnnotation(Acknowledgment.class);
            return annotation != null ? annotation.value() : null;
        });

        if (incoming != null) {
            this.incomingValue = incoming.value();
        }
        if (outgoing != null) {
            this.outgoingValue = outgoing.value();
        }

        MediaConfigurationSupport.ValidationOutput validationOutput = this.mediaConfigurationSupport.validate(this.shape,
                this.acknowledgment);
        this.production = validationOutput.getProduction();
        this.consumption = validationOutput.getConsumption();
        if (validationOutput.getUseBuilderTypes() != null) {
            this.useBuilderTypes = validationOutput.getUseBuilderTypes();
        }
        if (this.acknowledgment == null) {
            this.acknowledgment = this.mediaConfigurationSupport.processDefaultAcknowledgement(this.shape, this.consumption);
        }
        this.mergePolicy = this.mediaConfigurationSupport.processMerge(incoming, () -> {
            Merge annotation = method.getAnnotation(Merge.class);
            return annotation != null ? annotation.value() : null;
        });
        this.broadcastValue = this.mediaConfigurationSupport.processBroadcast(outgoing, () -> {
            Broadcast annotation = method.getAnnotation(Broadcast.class);
            return annotation != null ? annotation.value() : null;
        });
    }

    @Override
    public Shape shape() {
        return shape;
    }

    private void processBroadcast(Outgoing outgoing) {
        Broadcast bc = method.getAnnotation(Broadcast.class);
        if (outgoing != null) {
            if (bc != null) {
                this.broadcastValue = bc.value();
            }
        } else if (bc != null) {
            throw getIncomingError(
                    "The @Broadcast annotation is only supported for method annotated with @Outgoing: " + methodAsString());
        }
    }

    private IllegalArgumentException getOutgoingError(String message) {
        return new IllegalArgumentException("Invalid method annotated with @Outgoing: " + methodAsString() + " - " + message);
    }

    private IllegalArgumentException getIncomingError(String message) {
        return new IllegalArgumentException("Invalid method annotated with @Incoming: " + methodAsString() + " - " + message);
    }

    @Override
    public String getOutgoing() {
        return outgoingValue;
    }

    @Override
    public String getIncoming() {
        return incomingValue;
    }

    @Override
    public String methodAsString() {
        return mediatorBean.getBeanClass().getName() + "#" + method.getName();
    }

    @Override
    public Method getMethod() {
        return method;
    }

    @Override
    public Class<?> getReturnType() {
        return returnType;
    }

    @Override
    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    @Override
    public Consumption consumption() {
        return consumption;
    }

    @Override
    public Production production() {
        return production;
    }

    @Override
    public boolean usesBuilderTypes() {
        return useBuilderTypes;
    }

    @Override
    public Acknowledgment.Strategy getAcknowledgment() {
        return acknowledgment;
    }

    @Override
    public Merge.Mode getMerge() {
        return mergePolicy;
    }

    @Override
    public boolean getBroadcast() {
        return broadcastValue != null;
    }

    @Override
    public Bean<?> getBean() {
        return mediatorBean;
    }

    @Override
    public int getNumberOfSubscriberBeforeConnecting() {
        if (!getBroadcast()) {
            return -1;
        } else {
            return broadcastValue;
        }
    }

    @Override
    public Class<? extends Invoker> getInvokerClass() {
        return invokerClass;
    }

    static class ReflectionGenericTypeAssignable implements MediaConfigurationSupport.GenericTypeAssignable {

        private final Type type;

        public ReflectionGenericTypeAssignable(Type type) {
            this.type = type;
        }

        @Override
        public Result check(Class<?> target, int index) {
            if (!(type instanceof ParameterizedType)) {
                return Result.NotGeneric;
            }
            Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();
            if (arguments.length >= index + 1) {
                return TypeUtils.isAssignable(arguments[index], target) ? Result.Assignable : Result.NotAssignable;
            } else {
                return Result.InvalidIndex;
            }
        }
    }

    private static class ReturnTypeGenericTypeAssignable extends ReflectionGenericTypeAssignable {

        ReturnTypeGenericTypeAssignable(Method method) {
            super(method.getGenericReturnType());
        }
    }

    private static class AlwaysInvalidIndexGenericTypeAssignable implements MediaConfigurationSupport.GenericTypeAssignable {

        @Override
        public Result check(Class<?> target, int index) {
            return Result.InvalidIndex;
        }
    }

    private static class MethodParamGenericTypeAssignable extends ReflectionGenericTypeAssignable {

        MethodParamGenericTypeAssignable(Method method, int paramIndex) {
            super(getGenericParameterType(method, paramIndex));
        }

        private static Type getGenericParameterType(Method method, int paramIndex) {
            Type[] genericParameterTypes = method.getGenericParameterTypes();
            if (genericParameterTypes.length < paramIndex + 1) {
                throw new IllegalArgumentException("Method " + method + " only has " + genericParameterTypes.length
                        + " so parameter with index " + paramIndex + " cannot be retrieved");
            }
            return genericParameterTypes[paramIndex];
        }
    }

}
