package io.smallrye.reactive.messaging;

import org.apache.commons.lang3.ClassUtils;
import org.apache.logging.log4j.util.Strings;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

import javax.inject.Named;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public class MediatorConfiguration {

  private final Method method;

  private final Class<?> beanClass;

  private Incoming incoming;

  private Outgoing outgoing;

  private Class<?> returnedPayloadType;

  private Class<?> consumedPayloadType;
  private String mediatorName;

  private boolean consumeAsStream;

  public MediatorConfiguration(Method method, Class<?> beanClass) {
    this.method = Objects.requireNonNull(method, "'method' must be set");
    this.beanClass =  Objects.requireNonNull(beanClass, "'beanClass' must be set");
  }

  public MediatorConfiguration setOutgoing(Outgoing outgoing) {
    this.outgoing = outgoing;
    if (outgoing != null  && isVoid()) {
      throw new IllegalStateException("The method " + methodAsString() + " does not return a result but is annotated with @Outgoing. " +
        "The method must return 'something'");
    }
    returnedPayloadType = extractReturnedPayloadType();
    return this;
  }

  public MediatorConfiguration setIncoming(Incoming incoming) {
    this.incoming = incoming;
    consumedPayloadType = extractConsumedPayloadType();
    return this;
  }

  public String getName() {
    return mediatorName;
  }


  public String getOutgoingTopic() {
    if (outgoing == null) {
      return null;
    }
    return outgoing.topic();
  }

  public String getOutgoingProviderType() {
    if (outgoing == null) {
      return null;
    }
    // TODO Do we need to check if it's just MessagingProvider
    return outgoing.provider().getName();
  }

  public Class<?> getOutputType() {
    return returnedPayloadType;
  }

  private Class<?> extractReturnedPayloadType() {
    if (outgoing == null) {
      return null;
    }
    Class<?> type = method.getReturnType();
    ParameterizedType parameterizedType = null;
    if (method.getGenericReturnType() instanceof  ParameterizedType) {
      parameterizedType = (ParameterizedType) method.getGenericReturnType();
    }

    // We know that the method cannot return null at the point.
    // TODO We should still check for CompletionStage<Void>, or Publisher<Void> which would be invalid.

    if (parameterizedType == null) {
      return type;
    }

    if (ClassUtils.isAssignable(type, Publisher.class)
      || ClassUtils.isAssignable(type, Message.class)
      || ClassUtils.isAssignable(type, CompletionStage.class)
      || ClassUtils.isAssignable(type, PublisherBuilder.class)
      ) {
      // Extract the internal type - for all these type it's the first (unique) parameter
      Type enclosed = parameterizedType.getActualTypeArguments()[0];
      // TODO Should we do an instanceOf here?
      return (Class) enclosed;
    }

    if (ClassUtils.isAssignable(type, ProcessorBuilder.class)
     || ClassUtils.isAssignable(type, Processor.class)) {
      // Extract the internal type - for all these type it's the second parameter
      Type enclosed = parameterizedType.getActualTypeArguments()[1];
      // TODO Should we do an instanceOf here?
      return (Class) enclosed;
    }

    throw new IllegalStateException("Unable to determine the type of message returned by the method: " + methodAsString());
  }

  private Class<?> extractConsumedPayloadType() {
    if (incoming == null) {
      return null;
    }
    if (method.getParameterCount() == 0) {
      // The method must returned a ProcessorBuilder or a Processor, in this case, the consumed type is the first parameter.
      Class<?> type = method.getReturnType();
      ParameterizedType parameterizedType = null;
      if (method.getGenericReturnType() instanceof  ParameterizedType) {
        parameterizedType = (ParameterizedType) method.getGenericReturnType();
      }
      if (parameterizedType == null  || ! (ClassUtils.isAssignable(ProcessorBuilder.class, type) || ClassUtils.isAssignable(Processor.class, type))) {
        throw new IllegalStateException("Unable to determine the consumed type for " + methodAsString() + " - when the method does not has parameters, " +
          "the return type must be Processor or ProcessorBuilder.");
      }
      consumeAsStream = true;
      return (Class<?>) parameterizedType.getActualTypeArguments()[1];
    }

    if (method.getParameterCount() == 1) {
      // we need to check the parameter.
      Class<?> type = method.getParameterTypes()[0];
      ParameterizedType parameterizedType = null;
      if (method.getGenericParameterTypes()[0] instanceof  ParameterizedType) {
        parameterizedType = (ParameterizedType) method.getGenericParameterTypes()[0];
      }

      if (parameterizedType == null) {
        return type;
      }

      if (ClassUtils.isAssignable(type, Publisher.class)
        || ClassUtils.isAssignable(type, Message.class)
        || ClassUtils.isAssignable(type, PublisherBuilder.class)
        ) {
        // Extract the internal type - for all these type it's the first (unique) parameter
        consumeAsStream = ClassUtils.isAssignable(type, Publisher.class)  || ClassUtils.isAssignable(type, PublisherBuilder.class);
        Type enclosed = parameterizedType.getActualTypeArguments()[0];
        return (Class) enclosed;
      }

    }

    throw new IllegalStateException("Unable to determine the consumed type for " + methodAsString());
  }

  public String getIncomingTopic() {
    if (incoming == null) {
      return null;
    }
    if (Strings.isBlank(incoming.topic())) {
     throw new IllegalArgumentException("The @Incoming annotation must contain a non-blank topic");
    }
    return incoming.topic();
  }

  public String getIncomingProviderType() {
    if (incoming == null) {
      return null;
    }
    return incoming.provider().getName();
  }

  public Class<?> getIncomingType() {
    return consumedPayloadType;
  }

  public boolean isPublisher() {
    return returnedPayloadType != null;
  }

  public boolean isSubscriber() {
    return consumedPayloadType != null;
  }

  public boolean isProcessor() {
    return isPublisher()  && isSubscriber();
  }

  public Class<?> getReturnType() {
    if (! isVoid()) {
      return method.getReturnType();
    }
    return null;
  }

  public Class<?> getParameterType() {
    if (method.getParameterCount() == 1) {
      return method.getParameterTypes()[0];
    }
    return null;
  }

  public String getOutgoingName() {
   return mediatorName;
  }

  public String methodAsString() {
    return beanClass.getName() + "#" + method.getName();
  }

  private boolean isVoid() {
    return method.getReturnType().equals(Void.TYPE);
  }

  public MediatorConfiguration setNamed(Named named) {
    if (named != null) {
      mediatorName = named.value();
      // TODO Validation - only mediator with an outgoing annotation can be named
    }
    return this;
  }

  public Method getMethod() {
    return method;
  }

  public boolean isReturnTypeASubTypeOf(Class<?> clazz) {
    return ! isVoid() && isClassASubTypeOf(method.getReturnType(), clazz);
  }

  public static boolean isClassASubTypeOf(Class<?> maybeChild, Class<?> maybeParent) {
    return maybeParent.isAssignableFrom(maybeChild);
  }

  public Class<?> getBeanClass() {
    return beanClass;
  }

  public boolean consumeAsStream() {
    return consumeAsStream;
  }

  public boolean consumeItems() {
    return consumedPayloadType != null && ! consumedPayloadType.equals(Message.class);
  }

  public boolean produceItems() {
    return returnedPayloadType != null  && ! returnedPayloadType.equals(Message.class);
  }
}
