package io.smallrye.reactive.messaging;

import java.util.concurrent.CompletionStage;

public interface StreamRegistar {
  CompletionStage<Void> initialize();

}
