package io.smallrye.reactive.messaging.http;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.reactivex.Flowable;

public class HeaderPropagationTest extends HttpTestBase {

    @Test
    public void test() {
        stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        stubFor(post(urlEqualTo("/not-used"))
                .willReturn(aResponse()
                        .withStatus(500)));

        addConfig(new HttpConnectorConfig("http", "outgoing", "http://localhost:8089/not-used"));
        addClasses(MyApp.class);
        initialize();

        awaitForRequest(10);
        verify(10, postRequestedFor(urlEqualTo("/items")));
        assertThat(requests()).allSatisfy(req -> {
            assertThat(req.getBodyAsString()).isNotNull();
            assertThat(req.getAbsoluteUrl()).isEqualTo("http://localhost:8089/items");
            assertThat(req.getHeader("X-header")).isEqualTo("value");
        });
    }

    @ApplicationScoped
    public static class MyApp {

        @Outgoing("source")
        public Flowable<Integer> source() {
            return Flowable.range(0, 10);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return HttpMessage.<Integer> builder()
                    .withUrl("http://localhost:8089/items")
                    .withPayload(input.getPayload())
                    .withHeaders(Collections.singletonMap("X-header", Collections.singletonList("value")))
                    .build();
        }

        @Incoming("p1")
        @Outgoing("http")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

}
