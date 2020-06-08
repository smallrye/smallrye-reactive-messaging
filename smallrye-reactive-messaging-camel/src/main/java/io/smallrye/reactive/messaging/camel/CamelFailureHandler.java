package io.smallrye.reactive.messaging.camel;

import java.util.concurrent.CompletionStage;

public interface CamelFailureHandler {

    enum Strategy {
        FAIL,
        IGNORE;

        public static Strategy from(String s) {
            if (s == null || s.equalsIgnoreCase("fail")) {
                return FAIL;
            }
            if (s.equalsIgnoreCase("ignore")) {
                return IGNORE;
            }
            throw new IllegalArgumentException("Unknown failure strategy: " + s);
        }
    }

    CompletionStage<Void> handle(CamelMessage<?> message, Throwable reason);

}
