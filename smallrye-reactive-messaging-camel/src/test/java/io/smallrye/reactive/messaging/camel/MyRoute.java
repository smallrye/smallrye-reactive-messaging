package io.smallrye.reactive.messaging.camel;

import javax.enterprise.context.ApplicationScoped;

import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class MyRoute extends RouteBuilder {

    @Override
    public void configure() {
        from("direct:foo-in")
                .process(exchange -> exchange.getOut().setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                .to("reactive-streams:foo-out")
                .routeId("route-1");
        //    from("seda:input")
        //      .routeId("route-2")
        //      .to("file:./seda");
    }
}
