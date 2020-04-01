package acme;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {


    public static void main(String[] args) {
        SeContainer container = SeContainerInitializer.newInstance().initialize();
        final Sender sender = container.getBeanManager().createInstance().select(Sender.class).get();
        final Receiver receiver = container.getBeanManager().createInstance().select(Receiver.class).get();

        final AtomicInteger counter = new AtomicInteger();
        sender.send();
    }
}
