package acme;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

public class Main {

    public static void main(String[] args) {
        SeContainer container = SeContainerInitializer.newInstance().initialize();

        container.getBeanManager().createInstance().select(BeanUsingAnEmitter.class).get().periodicallySendMessageToKafka();
    }
}
