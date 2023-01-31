package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.spi.Bean;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.Invoker;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.providers.helpers.Validation;

public class DefaultMediatorConfiguration implements MediatorConfiguration {

    private final Bean<?> mediatorBean;

    private final Method method;

    private final Class<?> returnType;

    private final Class<?>[] parameterTypes;

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

    private final MediatorConfigurationSupport mediatorConfigurationSupport;

    private Type ingestedPayloadType;

    public DefaultMediatorConfiguration(Method method, Bean<?> bean) {
        this.method = Objects.requireNonNull(method, msg.methodMustBeSet());
        this.method.setAccessible(true);

        this.returnType = method.getReturnType();
        this.parameterTypes = method.getParameterTypes();
        this.mediatorBean = Objects.requireNonNull(bean, msg.beanMustBeSet());

        this.mediatorConfigurationSupport = new MediatorConfigurationSupport(methodAsString(), this.returnType,
                this.parameterTypes,
                new ReturnTypeGenericTypeAssignable(method),
                this.parameterTypes.length == 0 ? new AlwaysInvalidIndexGenericTypeAssignable()
                        : new MethodParamGenericTypeAssignable(method, 0));
    }

    public void compute(Incomings incomings, Outgoing outgoing, Blocking blocking) {
        Incoming[] values = incomings.value();
        if (values.length == 0) {
            throw ex.illegalArgumentForAnnotationNonEmpty("@Incoming", methodAsString());
        }
        compute(Arrays.asList(values), outgoing, blocking);
    }

    public void compute(List<Incoming> incomings, Outgoing outgoing, Blocking blocking) {
        if (incomings != null) {
            for (Incoming incoming : incomings) {
                if (Validation.isBlank(incoming.value())) {
                    throw ex.illegalArgumentForAnnotationNullOrBlank("@Incoming", methodAsString());
                }
            }
        } else {
            incomings = Collections.emptyList();
        }

        if (outgoing != null && Validation.isBlank(outgoing.value())) {
            throw ex.illegalArgumentForAnnotationNullOrBlank("@Outgoing", methodAsString());
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
            if (!blocking.value().equals(Blocking.DEFAULT_WORKER_POOL)) {
                this.workerPoolName = blocking.value();
            }
        }

        MediatorConfigurationSupport.ValidationOutput validationOutput = this.mediatorConfigurationSupport.validate(this.shape,
                this.acknowledgment);
        this.production = validationOutput.getProduction();
        this.consumption = validationOutput.getConsumption();
        if (validationOutput.getUseBuilderTypes()) {
            this.useBuilderTypes = validationOutput.getUseBuilderTypes();
        }
        if (this.acknowledgment == null) {
            this.acknowledgment = this.mediatorConfigurationSupport.processDefaultAcknowledgement(this.shape, this.consumption,
                    this.production);
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

        ingestedPayloadType = validationOutput.getIngestedPayloadType();
    }

    @Override
    public Shape shape() {
        return shape;
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
    public Type getIngestedPayloadType() {
        return ingestedPayloadType;
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
    public boolean isBlockingExecutionOrdered() {
        return isOrderedExecution;
    }

    @Override
    public Class<? extends Invoker> getInvokerClass() {
        return null;
    }

    public void strict() {
        this.mediatorConfigurationSupport.strict();
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

        @Override
        public Type getType(int index) {
            return extractGenericType(type, index);
        }

        private Type extractGenericType(Type owner, int index) {
            if (!(owner instanceof ParameterizedType)) {
                return null;
            }
            Type[] arguments = ((ParameterizedType) owner).getActualTypeArguments();
            if (arguments.length >= index + 1) {
                Type result = arguments[index];
                if (result instanceof WildcardType) {
                    return null;
                }

                return result;
            } else {
                return null;
            }
        }

        @Override
        public Type getType(int index, int subIndex) {
            Type generic = extractGenericType(this.type, index);
            if (generic != null) {
                return extractGenericType(generic, subIndex);
            } else {
                return null;
            }
        }
    }

    public static class ReturnTypeGenericTypeAssignable extends ReflectionGenericTypeAssignable {

        public ReturnTypeGenericTypeAssignable(Method method) {
            super(method.getGenericReturnType());
        }
    }

    public static class AlwaysInvalidIndexGenericTypeAssignable
            implements MediatorConfigurationSupport.GenericTypeAssignable {

        @Override
        public Result check(Class<?> target, int index) {
            return Result.InvalidIndex;
        }

        @Override
        public Type getType(int index) {
            return null;
        }

        @Override
        public Type getType(int index, int subIndex) {
            return null;
        }
    }

    public static class MethodParamGenericTypeAssignable extends ReflectionGenericTypeAssignable {

        public MethodParamGenericTypeAssignable(Method method, int paramIndex) {
            super(getGenericParameterType(method, paramIndex));
        }

        private static Type getGenericParameterType(Method method, int paramIndex) {
            Type[] genericParameterTypes = method.getGenericParameterTypes();
            if (genericParameterTypes.length < paramIndex + 1) {
                throw ex.illegalArgumentForGenericParameterType(method, genericParameterTypes.length,
                        paramIndex);
            }
            return genericParameterTypes[paramIndex];
        }
    }

}
