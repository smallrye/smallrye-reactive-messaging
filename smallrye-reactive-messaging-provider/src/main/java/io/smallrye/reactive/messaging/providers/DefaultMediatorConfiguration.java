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
import io.smallrye.reactive.messaging.MethodParameterDescriptor;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.annotations.Outgoings;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.keyed.Keyed;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.smallrye.reactive.messaging.providers.helpers.Validation;

public class DefaultMediatorConfiguration implements MediatorConfiguration {

    private final Bean<?> mediatorBean;

    private final Method method;

    private final Class<?> returnType;

    private final Class<?>[] parameterTypes;

    private Shape shape;

    private List<String> incomingValues = Collections.emptyList();

    private List<String> outgoingValues = Collections.emptyList();

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

    private boolean useReactiveStreams = false;

    private boolean hasTargetedOutput = false;

    /**
     * The merge policy.
     */
    private Merge.Mode mergePolicy;

    private boolean isBlocking = false;

    private String workerPoolName = null;

    private boolean isOrderedExecution;

    private final MediatorConfigurationSupport mediatorConfigurationSupport;

    private Type ingestedPayloadType;
    /**
     * Only relevant for the KeyedMulti case.
     */
    private Type keyType;
    /**
     * Only relevant for the KeyedMulti case.
     */
    private Type valueType;

    /**
     * Only relevant for the KeyedMulti case.
     */
    private Class<? extends KeyValueExtractor> keyed;

    private final MethodParameterDescriptor descriptor;

    public DefaultMediatorConfiguration(Method method, Bean<?> bean) {
        this.method = Objects.requireNonNull(method, msg.methodMustBeSet());
        this.method.setAccessible(true);

        this.returnType = method.getReturnType();
        this.parameterTypes = method.getParameterTypes();
        Class<? extends KeyValueExtractor> keyed = null;
        if (method.getParameters().length == 1 && method.getParameters()[0].getAnnotation(Keyed.class) != null) {
            keyed = method.getParameters()[0].getAnnotation(Keyed.class).value();
        }

        this.mediatorBean = Objects.requireNonNull(bean, msg.beanMustBeSet());

        MediatorConfigurationSupport.GenericTypeAssignable[] params = new MediatorConfigurationSupport.GenericTypeAssignable[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            params[i] = new MethodParamGenericTypeAssignable(method, i);
        }

        this.descriptor = new MethodParameterDescriptor() {
            @Override
            public List<Class<?>> getTypes() {
                return Arrays.stream(parameterTypes).collect(Collectors.toList());
            }

            @Override
            public Class<?> getGenericParameterType(int paramIndex, int genericIndex) {
                Type type = params[paramIndex].getType(genericIndex);
                if (type instanceof Class) {
                    return (Class<?>) type;
                }
                try {
                    return DefaultMediatorConfiguration.class.getClassLoader().loadClass(type.getTypeName());
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        this.mediatorConfigurationSupport = new MediatorConfigurationSupport(methodAsString(), this.returnType,
                this.parameterTypes,
                new ReturnTypeGenericTypeAssignable(method),
                this.parameterTypes.length == 0 ? new AlwaysInvalidIndexGenericTypeAssignable()
                        : new MethodParamGenericTypeAssignable(method, 0));
        this.keyed = keyed;
    }

    public void compute(Incomings incomings, Outgoing outgoing, Blocking blocking) {
        Incoming[] values = incomings.value();
        if (values.length == 0) {
            throw ex.illegalArgumentForAnnotationNonEmpty("@Incoming", methodAsString());
        }
        compute(Arrays.asList(values), Collections.singletonList(outgoing), blocking);
    }

    public void compute(Incomings incomings, Outgoings outgoings, Blocking blocking) {
        Incoming[] ins = incomings.value();
        if (ins.length == 0) {
            throw ex.illegalArgumentForAnnotationNonEmpty("@Incoming", methodAsString());
        }
        Outgoing[] outs = outgoings.value();
        if (outs.length == 0) {
            throw ex.illegalArgumentForAnnotationNonEmpty("@Outgoing", methodAsString());
        }
        compute(Arrays.asList(ins), Arrays.asList(outs), blocking);
    }

    public void compute(Incomings incomings, List<Outgoing> outgoings, Blocking blocking) {
        Incoming[] ins = incomings.value();
        if (ins.length == 0) {
            throw ex.illegalArgumentForAnnotationNonEmpty("@Incoming", methodAsString());
        }
        compute(Arrays.asList(ins), outgoings, blocking);
    }

    public void compute(Incoming incoming, Outgoings outgoings, Blocking blocking) {
        Outgoing[] values = outgoings.value();
        if (values.length == 0) {
            throw ex.illegalArgumentForAnnotationNonEmpty("@Outgoing", methodAsString());
        }
        compute(Collections.singletonList(incoming), Arrays.asList(values), blocking);
    }

    public void compute(List<Incoming> incomings, Outgoing outgoing, Blocking blocking) {
        compute(incomings, Collections.singletonList(outgoing), blocking);
    }

    public void compute(List<Incoming> incomings, Outgoings outgoings, Blocking blocking) {
        Outgoing[] outs = outgoings.value();
        if (outs.length == 0) {
            throw ex.illegalArgumentForAnnotationNonEmpty("@Outgoing", methodAsString());
        }
        compute(incomings, Arrays.asList(outs), blocking);
    }

    public void compute(List<Incoming> incomings, List<Outgoing> outgoings, Blocking blocking) {
        if (incomings != null) {
            for (Incoming incoming : incomings) {
                if (Validation.isBlank(incoming.value())) {
                    throw ex.illegalArgumentForAnnotationNullOrBlank("@Incoming", methodAsString());
                }
            }
        } else {
            incomings = Collections.emptyList();
        }

        if (outgoings != null) {
            for (Outgoing outgoing : outgoings) {
                if (Validation.isBlank(outgoing.value())) {
                    throw ex.illegalArgumentForAnnotationNullOrBlank("@Outgoing", methodAsString());
                }
            }
        }

        this.shape = this.mediatorConfigurationSupport.determineShape(incomings, outgoings);

        this.acknowledgment = this.mediatorConfigurationSupport.processSuppliedAcknowledgement(incomings, () -> {
            Acknowledgment annotation = method.getAnnotation(Acknowledgment.class);
            return annotation != null ? annotation.value() : null;
        });

        if (!incomings.isEmpty()) {
            this.incomingValues = incomings.stream().map(Incoming::value).collect(Collectors.toList());
        }
        if (!outgoings.isEmpty()) {
            this.outgoingValues = outgoings.stream().map(Outgoing::value).collect(Collectors.toList());
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
        this.useBuilderTypes = validationOutput.getUseBuilderTypes();
        this.useReactiveStreams = validationOutput.getUseReactiveStreams();
        if (this.acknowledgment == null) {
            this.acknowledgment = this.mediatorConfigurationSupport.processDefaultAcknowledgement(this.shape, this.consumption,
                    this.production);
        }
        this.hasTargetedOutput = this.mediatorConfigurationSupport.processTargetedOutput();
        this.mergePolicy = this.mediatorConfigurationSupport.processMerge(incomings, () -> {
            Merge annotation = method.getAnnotation(Merge.class);
            return annotation != null ? annotation.value() : null;
        });
        this.broadcastValue = this.mediatorConfigurationSupport.processBroadcast(outgoings, () -> {
            Broadcast annotation = method.getAnnotation(Broadcast.class);
            return annotation != null ? annotation.value() : null;
        });

        if (this.isBlocking) {
            this.mediatorConfigurationSupport.validateBlocking(validationOutput);
        }

        ingestedPayloadType = validationOutput.getIngestedPayloadType();
        keyType = validationOutput.getKeyType();
        valueType = validationOutput.getValueType();
    }

    @Override
    public Shape shape() {
        return shape;
    }

    @Override
    public String getOutgoing() {
        // Backwards compatible
        if (outgoingValues != null && !outgoingValues.isEmpty()) {
            return outgoingValues.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<String> getOutgoings() {
        // Backwards compatible
        if (outgoingValues != null && !outgoingValues.isEmpty()) {
            return outgoingValues;
        } else {
            return Collections.emptyList();
        }
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
    public MethodParameterDescriptor getParameterDescriptor() {
        return descriptor;
    }

    @Override
    public boolean hasTargetedOutput() {
        return hasTargetedOutput;
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
    public boolean usesReactiveStreams() {
        return useReactiveStreams;
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

    @Override
    public Type getKeyType() {
        return keyType;
    }

    @Override
    public Type getValueType() {
        return valueType;
    }

    @Override
    public Class<? extends KeyValueExtractor> getKeyed() {
        return keyed;
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
