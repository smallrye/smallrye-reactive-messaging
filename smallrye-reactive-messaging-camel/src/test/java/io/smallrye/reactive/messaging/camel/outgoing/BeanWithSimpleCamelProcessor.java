package io.smallrye.reactive.messaging.camel.outgoing;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class BeanWithSimpleCamelProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BeanWithSimpleCamelProcessor.class);

    @Incoming("in")
    @Outgoing("out")
    public byte[] extract(byte[] value) {
        LOG.info("Received message {}", value);
        return value;
    }

}
