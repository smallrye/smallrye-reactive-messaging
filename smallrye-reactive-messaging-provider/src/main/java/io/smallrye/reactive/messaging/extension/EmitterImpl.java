package io.smallrye.reactive.messaging.extension;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.smallrye.reactive.messaging.annotations.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class EmitterImpl<T> implements Emitter<T> {

  private final AtomicReference<FlowableEmitter<Message<? extends T>>> internal = new AtomicReference<>();

  EmitterImpl(Subscriber<Message< ? extends T>> subscriber) {
    Objects.requireNonNull(subscriber);
    Flowable<Message<? extends T>> flowable = Flowable.create(x -> {
      if (!internal.compareAndSet(null, x)) {
        x.onError(new Exception("Emitter already created"));
      }
    }, BackpressureStrategy.BUFFER);
    flowable.subscribe(subscriber);
    if (internal.get() == null) {
      throw new IllegalStateException("Unable to connect to the stream");
    }
  }

  @Override
  public Emitter<T> send(T msg) {
    FlowableEmitter<Message<? extends T>> emitter = verify();
    if (msg == null) {
      throw new IllegalArgumentException("`null` is not a valid value");
    }
    if (msg instanceof Message) {
      //noinspection unchecked
      emitter.onNext((Message) msg);
    } else {
      emitter.onNext(Message.of(msg));
    }
    return this;
  }

  private FlowableEmitter<Message<? extends T>> verify() {
    FlowableEmitter<Message<? extends T>> emitter = internal.get();
    if (emitter == null) {
      throw new IllegalStateException("Stream not yet connected");
    }
    return emitter;
  }

  @Override
  public void complete() {
    verify().onComplete();
  }

  @Override
  public void error(Exception e) {
    if (e == null) {
      throw new IllegalArgumentException("`null` is not a valid exception");
    }
    verify().onError(e);
  }
}
