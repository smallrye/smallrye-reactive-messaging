package camel.api;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class CamelApi {

    // <reactive>
    @Inject
    CamelReactiveStreamsService reactiveStreamsService;
    // </reactive>

    // <source>
    @Outgoing("camel")
    public Publisher<Exchange> retrieveDataFromCamelRoute() {
        return reactiveStreamsService.from("seda:camel");
    }
    // </source>

    // <source-route-builder>
    @ApplicationScoped
    static class MyRouteBuilder extends RouteBuilder {
        @Inject
        CamelReactiveStreamsService reactiveStreamsService;

        @Outgoing("sink")
        public Publisher<String> getDataFromCamelRoute() {
            return reactiveStreamsService.fromStream("my-stream", String.class);
        }

        @Override
        public void configure() {
            from("seda:camel").process(
                    exchange -> exchange.getMessage().setBody(exchange.getIn().getBody(String.class).toUpperCase()))
                    .to("reactive-streams:my-stream");
        }
    }
    // </source-route-builder>

    // <sink>
    @Incoming("to-camel")
    public Subscriber<String> sendDataToCamelRoute() {
        return reactiveStreamsService.subscriber("file:./target?fileName=values.txt&fileExist=append",
                String.class);
    }
    // </sink>

    // <producer>
    @Inject
    CamelContext camel;

    @Incoming("to-camel")
    public CompletionStage<Void> sink(String value) {
        return camel.createProducerTemplate()
                .asyncSendBody("file:./target?fileName=values.txt&fileExist=append", value).thenApply(x -> null);
    }
    // </producer>
}
