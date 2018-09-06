package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public abstract class AbstractMediator {

  protected final MediatorConfiguration configuration;


  public AbstractMediator(MediatorConfiguration configuration) {
    this.configuration = configuration;
  }

  public void run() {
    // Do nothing by default.
  }

  public void connectToUpstream(Publisher<? extends Message> publisher) {
    // Do nothing by default.
  }

  public abstract void initialize(Object bean);

  @SuppressWarnings("unchecked")
  protected <T> T invoke(Object bean, Object... args) {
    try {
      Method method = configuration.getMethod();
      return (T) method.invoke(bean, args);
    } catch (InvocationTargetException e) {
      throw new ProcessingException(configuration.methodAsString(), e.getTargetException());
    } catch (Exception e) {
      throw new ProcessingException(configuration.methodAsString(), e);
    }
  }

  protected CompletionStage<? extends Void> getAckOrCompletion(Message<?> message) {
    CompletionStage<Void> ack = message.ack();
    if (ack != null) {
      return ack;
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  public Publisher<Message> getComputedPublisher() {
    return null;
  }

  public MediatorConfiguration getConfiguration() {
    return configuration;
  }

  public String getMethodAsString() {
    return configuration.methodAsString();
  }

  public Subscriber<Message> getComputedSubscriber() {
    return null;
  }

  public abstract boolean isConnected();
}
