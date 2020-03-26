package io.smallrye.reactive.messaging;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.enterprise.inject.spi.Bean;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.helpers.TypeUtils;
import io.smallrye.reactive.messaging.helpers.Validation;

public class DefaultMediatorConfiguration implements MediatorConfiguration {

    private final Bean<?> mediatorBean;

    private final Method method;

    private Class<?> returnType;

    private Class<?>[] parameterTypes;

    private Shape shape;

    private List<String> incomingValues = Collections.emptyList();

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

    private boolean isBlocking = false;

    private String workerPoolName = null;

    private boolean isOrderedExecution;

    private MediatorConfigurationSupport mediatorConfigurationSupport;

    public DefaultMediatorConfiguration(Method method, Bean<?> bean) {
        this.method = Objects.requireNonNull(method, "'method' must be set");
        this.returnType = method.getReturnType();
        this.parameterTypes = method.getParameterTypes();
        this.mediatorBean = Objects.requireNonNull(bean, "'bean' must be set");

        this.mediatorConfigurationSupport = new MediatorConfigurationSupport(methodAsString(), this.returnType,
                this.parameterTypes,
                new ReturnTypeGenericTypeAssignable(method),
                this.parameterTypes.length == 0 ? new AlwaysInvalidIndexGenericTypeAssignable()
                        : new MethodParamGenericTypeAssignable(method, 0));
    }

    public void compute(Incomings incomings, Outgoing outgoing, Blocking blocking) {
        Incoming[] values = incomings.value();
        if (values.length == 0) {
            throw getIncomingError("@Incomings must contain a non-empty array of @Incoming");
        }
        compute(Arrays.asList(values), outgoing, blocking);
    }

    public void compute(List<Incoming> incomings, Outgoing outgoing, Blocking blocking) {
        if (incomings != null) {
            for (Incoming incoming : incomings) {
                if (Validation.isBlank(incoming.value())) {
                    throw getIncomingError("value is blank or null");
                }
            }
        } else {
            incomings = Collections.emptyList();
        }

        if (outgoing != null && Validation.isBlank(outgoing.value())) {
            throw getOutgoingError("value is blank or null");
        }

        this.shape = this.mediatorConfigurationSupport.determineShape(incomings, outgoing);

        this.acknowledgment = this.mediatorConfigurationSupport.processSuppliedAcknowledgement(incomings, () -> {
            Acknowledgment annotation = method.getAnnotation(Acknowledgment.class);
            return annotation != null ? annotation.value() : null;
        });

        if (!incomings.isEmpty()) {
            this.incomingValues = incomings.stream().map(Incoming::value).collect(Collectors.toList());
        }
        if (outgoing != null) {
            this.outgoingValue = outgoing.value();
        }
        if (blocking != null) {
            this.isBlocking = true;
            this.isOrderedExecution = blocking.ordered();
            if (!blocking.value().equals(Blocking.NO_VALUE)) {
                this.workerPoolName = blocking.value();
            }
        }

        MediatorConfigurationSupport.ValidationOutput validationOutput = this.mediatorConfigurationSupport.validate(this.shape,
                this.acknowledgment);
        this.production = validationOutput.getProduction();
        this.consumption = validationOutput.getConsumption();
        if (validationOutput.getUseBuilderTypes() != null) {
            this.useBuilderTypes = validationOutput.getUseBuilderTypes();
        }
        if (this.acknowledgment == null) {
            this.acknowledgment = this.mediatorConfigurationSupport.processDefaultAcknowledgement(this.shape, this.consumption);
        }
        this.mergePolicy = this.mediatorConfigurationSupport.processMerge(incomings, () -> {
            Merge annotation = method.getAnnotation(Merge.class);
            return annotation != null ? annotation.value() : null;
        });
        this.broadcastValue = this.mediatorConfigurationSupport.processBroadcast(outgoing, () -> {
            Broadcast annotation = method.getAnnotation(Broadcast.class);
            return annotation != null ? annotation.value() : null;
        });

        if (this.isBlocking) {
            this.mediatorConfigurationSupport.validateBlocking(validationOutput);
        }
    }

    @Override
    public Shape shape() {
        return shape;
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
    public List<String> getIncoming() {
        return incomingValues;
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
    public boolean isBlocking() {
        return isBlocking;
    }

    @Override
    public String getWorkerPoolName() {
        return workerPoolName;
    }

    @Override
    public boolean isOrderedExecution() {
        return isOrderedExecution;
    }

    @Override
    public Class<? extends Invoker> getInvokerClass() {
        return null;
    }

    static class ReflectionGenericTypeAssignable implements MediatorConfigurationSupport.GenericTypeAssignable {

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

    private static class AlwaysInvalidIndexGenericTypeAssignable implements MediatorConfigurationSupport.GenericTypeAssignable {

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
