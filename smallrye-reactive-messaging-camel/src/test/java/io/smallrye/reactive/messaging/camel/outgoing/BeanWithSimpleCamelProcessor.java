package io.smallrye.reactive.messaging.camel.outgoing;

import javax.inject.Inject;

import org.apache.camel.Exchange;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class BeanWithSimpleCamelProcessor {

    @Inject
    private CamelReactiveStreamsService camel;

    @Incoming("camel")
    @Outgoing("sink")
    public String extract(Exchange exchange) {
        return exchange.getIn().getBody(String.class);
    }

}
