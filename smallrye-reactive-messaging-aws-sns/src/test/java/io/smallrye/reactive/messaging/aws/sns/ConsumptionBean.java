package io.smallrye.reactive.messaging.aws.sns;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class ConsumptionBean {

    public static String msgReceived;

    @Incoming("source")
    public String consume(String message) {
        msgReceived = message;
        return message.toUpperCase();
    }
}
