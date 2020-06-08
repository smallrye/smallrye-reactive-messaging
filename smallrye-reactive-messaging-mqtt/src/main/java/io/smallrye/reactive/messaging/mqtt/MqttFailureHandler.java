package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;

import java.util.concurrent.CompletionStage;

public interface MqttFailureHandler {

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
            throw ex.illegalArgumentUnknownStrategy(s);
        }
    }

    CompletionStage<Void> handle(Throwable reason);

}
