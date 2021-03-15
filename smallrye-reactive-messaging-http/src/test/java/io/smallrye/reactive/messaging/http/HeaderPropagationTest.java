package io.smallrye.reactive.messaging.http;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;

public class HeaderPropagationTest extends HttpTestBase {

    @Test
    public void test() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        wireMockServer.stubFor(post(urlEqualTo("/not-used"))
                .willReturn(aResponse()
                        .withStatus(500)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/not-used"));
        addClasses(MyApp.class);
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));
        assertThat(requests()).hasSize(10).allSatisfy(req -> {
            assertThat(req.getBodyAsString()).isNotNull();
            assertThat(req.getAbsoluteUrl()).isEqualTo(getHost() + "/items");
            assertThat(req.getHeader("X-header")).isEqualTo("value");
        });
    }

    @ApplicationScoped
    public static class MyApp {

        @Outgoing("source")
        public Multi<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return HttpMessage.<Integer> builder()
                    .withUrl(getHost() + "/items")
                    .withPayload(input.getPayload())
                    .withHeaders(Collections.singletonMap("X-header", Collections.singletonList("value")))
                    .withAck(input::ack)
                    .build();
        }

        @Incoming("p1")
        @Outgoing("http")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

}
