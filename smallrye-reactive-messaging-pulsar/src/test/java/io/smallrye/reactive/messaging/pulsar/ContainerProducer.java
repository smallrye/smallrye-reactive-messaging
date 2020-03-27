package io.smallrye.reactive.messaging.pulsar;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.testcontainers.containers.PulsarContainer;

import javax.enterprise.context.SessionScoped;
import javax.enterprise.inject.Produces;
import java.io.Serializable;

@SessionScoped
public class ContainerProducer implements Serializable {



    @ConfigProperty(name = "io.smallrye.reactive.messaging.pulsar.test.container.image")
    String imageName;

    @Produces
    public PulsarContainer pulsarContainer(){
        PulsarContainer pulsarContainer = new PulsarContainer(imageName);
        return pulsarContainer;
    }
}
