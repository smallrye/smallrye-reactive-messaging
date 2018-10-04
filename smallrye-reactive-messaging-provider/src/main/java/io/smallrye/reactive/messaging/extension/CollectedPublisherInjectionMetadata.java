package io.smallrye.reactive.messaging.extension;

import io.reactivex.Flowable;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.reactivestreams.Publisher;

import javax.enterprise.util.TypeLiteral;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

class CollectedPublisherInjectionMetadata {

  private final String name;
  private boolean needFlowableOfMessageBean = false;
  private boolean needFlowableOfPayloadBean = false;
  private boolean needPublisherBuilderOfMessageBean = false;
  private boolean needPublisherBuilderOfPayloadBean = false;

  private Type messageTypeForFlowable;
  private Type payloadTypeForFlowable;
  private Type messageTypeForPublisherBuilder;
  private Type payloadTypeForPublisherBuilder;
  private Type messageTypeForPublisher;
  private ParameterizedType payloadTypeForPublisher;


  public CollectedPublisherInjectionMetadata(String name) {
    this.name = name;
  }

  public void setType(Type type) {
    if (TypeUtils.isAssignable(type, Publisher.class)) {
      // Flowable
      Type first = getFirstParameter(type);
      if (first != null) {
        if (TypeUtils.isAssignable(first, Message.class)) {
          needFlowableOfMessageBean = true;
          Type nested = getFirstParameter(first);
          if (nested == null  && messageTypeForFlowable == null  || nested != null) {
            messageTypeForFlowable = TypeUtils.parameterize(Flowable.class, first);
            messageTypeForPublisher = TypeUtils.parameterize(Publisher.class, first);
          }
        } else {
          needFlowableOfPayloadBean = true;
          Type nested = getFirstParameter(first);
          if (nested == null  && payloadTypeForFlowable == null  || nested != null) {
            payloadTypeForFlowable = TypeUtils.parameterize(Flowable.class, first);
            payloadTypeForPublisher = TypeUtils.parameterize(Publisher.class, first);
          }
        }
      }
    } else {
      // Publisher Builder
      Type first = getFirstParameter(type);
      if (first != null) {
        if (TypeUtils.isAssignable(first, Message.class)) {
          needPublisherBuilderOfMessageBean = true;
          Type nested = getFirstParameter(first);
          if (nested == null  && messageTypeForPublisherBuilder == null  || nested != null) {
            messageTypeForPublisherBuilder = TypeUtils.parameterize(PublisherBuilder.class, first);
          }
        } else {
          needPublisherBuilderOfPayloadBean = true;
          Type nested = getFirstParameter(first);
          if (nested == null  && payloadTypeForPublisherBuilder == null  || nested != null) {
            payloadTypeForPublisherBuilder = TypeUtils.parameterize(PublisherBuilder.class, first);
          }
        }
      }


    }
  }

  private Type getFirstParameter(Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getActualTypeArguments()[0];
    }
    return null;
  }

  public boolean needFlowableOfMessageBean() {
    return needFlowableOfMessageBean;
  }

  public boolean needFlowableOfPayloadBean() {
    return needFlowableOfPayloadBean;
  }

  public boolean needPublisherBuilderOfMessageBean() {
    return needPublisherBuilderOfMessageBean;
  }

  public boolean isNeedPublisherBuilderOfPayloadBean() {
    return needPublisherBuilderOfPayloadBean;
  }

  public Type getMessageTypeForFlowable() {
    return messageTypeForFlowable;
  }

  public Type getPayloadTypeForFlowable() {
    return payloadTypeForFlowable;
  }

  public Type getMessageTypeForPublisherBuilder() {
    return messageTypeForPublisherBuilder;
  }

  public Type getPayloadTypeForPublisherBuilder() {
    return payloadTypeForPublisherBuilder;
  }

  public String getName() {
    return name;
  }

  public Type getMessageTypeForPublisher() {
    return messageTypeForPublisher;
  }

  public Type getPayloadTypeForPublisher() {
    return payloadTypeForPublisher;
  }
}
