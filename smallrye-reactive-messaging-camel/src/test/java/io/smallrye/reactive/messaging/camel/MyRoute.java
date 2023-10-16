package io.smallrye.reactive.messaging.camel;

import jakarta.inject.Singleton;

import org.apache.camel.builder.RouteBuilder;

@Singleton
public class MyRoute extends RouteBuilder {

    @Override
    public void configure() {
        from("direct:foo-in")
                .process(exchange -> exchange.getMessage()
                        .setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                .to("reactive-streams:foo-out")
                .routeId("route-1");
    }
}
