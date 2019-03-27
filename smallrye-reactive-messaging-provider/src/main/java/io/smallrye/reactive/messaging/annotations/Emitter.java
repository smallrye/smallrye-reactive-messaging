package io.smallrye.reactive.messaging.annotations;

public interface Emitter<T> {

  Emitter<T> send(T msg);

  void complete();

  void error(Exception e);

}
