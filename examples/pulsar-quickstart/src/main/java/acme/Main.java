package acme;

/*
TODO : https://github.com/rafaelsousa/smallrye-reactive-messaging/issues/15
 */
//import org.eclipse.microprofile.reactive.messaging.Incoming;
//import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

public class Main {


    public static void main(String[] args) {
        SeContainer container = SeContainerInitializer.newInstance().initialize();
    }

//    @Incoming("producer")
    public void consume(byte [] message) {
        System.out.println("received: " + String.valueOf(message));
    }


//    @Outgoing("consumer")
    public String send() {
        return "Message";
    }
}
