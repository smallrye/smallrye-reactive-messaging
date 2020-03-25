package api;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletionStage;

public class CamelApi {

    // tag::reactive[]
    @Inject CamelReactiveStreamsService reactiveStreamsService;
    // end::reactive[]

    // tag::source[]
    @Outgoing("camel")
    public Publisher<Exchange> retrieveDataFromCamelRoute() {
        return reactiveStreamsService.from("seda:camel");
    }
    // end::source[]

    // tag::source-route-builder[]
    @ApplicationScoped
    static class MyRouteBuilder extends RouteBuilder {
        @Inject CamelReactiveStreamsService reactiveStreamsService;

        @Outgoing("sink")
        public Publisher<String> getDataFromCamelRoute() {
            return reactiveStreamsService.fromStream("my-stream", String.class);
        }

        @Override
        public void configure() throws Exception {
            from("seda:camel").process(
                exchange -> exchange.getMessage().setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                .to("reactive-streams:my-stream");
        }
    }
    // end::source-route-builder[]

    // tag::sink[]
    @Incoming("to-camel")
    public Subscriber<String> sendDataToCamelRoute() {
        return reactiveStreamsService.subscriber("file:./target?fileName=values.txt&fileExist=append",
            String.class);
    }
    // end::sink[]


    // tag::producer[]
    @Inject CamelContext camel;

    @Incoming("to-camel")
    public CompletionStage<Void> sink(String value) {
        return camel.createProducerTemplate()
            .asyncSendBody("file:./target?fileName=values.txt&fileExist=append", value).thenApply(x -> null);
    }
    // end::producer[]
}
