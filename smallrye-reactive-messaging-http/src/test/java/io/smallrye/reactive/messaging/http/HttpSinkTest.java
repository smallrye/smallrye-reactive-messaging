package io.smallrye.reactive.messaging.http;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.http.Fault;

import io.smallrye.mutiny.Multi;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class HttpSinkTest extends HttpTestBase {

    @Test
    public void testABeanProducingJsonObjects() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingJsonObjects.class, SourceBean.class);
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));

        bodies("/items").stream()
                .map(JsonObject::new)
                .forEach(json -> assertThat(json.getMap()).containsKey("value"));

        assertThat(bodies("/items").stream()
                .map(JsonObject::new)
                .map(j -> j.getInteger("value"))
                .collect(Collectors.toList())).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testABeanProducingMessagesOfJsonObject() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingMessagesOfJsonObject.class, SourceBean.class);
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));

        bodies("/items").stream()
                .map(JsonObject::new)
                .forEach(json -> assertThat(json.getMap()).containsKey("value"));

        assertThat(bodies("/items").stream()
                .map(JsonObject::new)
                .map(j -> j.getInteger("value"))
                .collect(Collectors.toList())).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testABeanProducingJsonObjectsWithLatency() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withLogNormalRandomDelay(90, 0.1)
                        .withStatus(204)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingJsonObjects.class, SourceBean.class);
        initialize();

        awaitForRequest(10, 60000);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));

        bodies("/items").stream()
                .map(JsonObject::new)
                .forEach(json -> assertThat(json.getMap()).containsKey("value"));

        assertThat(bodies("/items").stream()
                .map(JsonObject::new)
                .map(j -> j.getInteger("value"))
                .collect(Collectors.toList())).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testABeanProducingJsonObjectsWith500Error() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(500)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingJsonObjects.class, SourceBean.class);
        initialize();

        awaitForRequest(1);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/items")));

        bodies("/items").stream()
                .map(JsonObject::new)
                .forEach(json -> assertThat(json.getMap()).containsKey("value"));

        assertThat(bodies("/items").stream()
                .map(JsonObject::new)
                .map(j -> j.getInteger("value"))
                .collect(Collectors.toList())).containsExactlyInAnyOrder(1);
    }

    @Test
    public void testABeanProducingJsonObjectsWithFault() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingJsonObjects.class, SourceBean.class);
        initialize();

        awaitForRequest(1);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/items")));

        bodies("/items").stream()
                .map(JsonObject::new)
                .forEach(json -> assertThat(json.getMap()).containsKey("value"));

        assertThat(bodies("/items").stream()
                .map(JsonObject::new)
                .map(j -> j.getInteger("value"))
                .collect(Collectors.toList())).containsExactlyInAnyOrder(1);
    }

    @Test
    public void testABeanProducingJsonArrays() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingJsonArrays.class, SourceBean.class);
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));

        bodies("/items").stream()
                .map(JsonArray::new)
                .forEach(json -> assertThat(json).hasSize(2));
    }

    @Test
    public void testABeanProducingBytes() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addClasses(BeanProducingBytes.class, SourceBean.class);
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));

        assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8", "9",
                "10");
    }

    @Test
    public void testABeanProducingByteBuffers() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingByteBuffers.class, SourceBean.class);
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));

        assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8", "9",
                "10");
    }

    @Test
    public void testABeanProducingVertxBuffers() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingVertxBuffers.class, SourceBean.class);
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, postRequestedFor(urlEqualTo("/items")));

        assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8", "9",
                "10");
    }

    @Test
    public void testABeanUsingCustomCodec() {
        wireMockServer.stubFor(post(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addClasses(BeanProducingPersons.class, SourceBeanWithConverter.class);
        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items")
                .converter(PersonSerializer.class.getName()));
        initialize();

        awaitForRequest(3);
        wireMockServer.verify(3, postRequestedFor(urlEqualTo("/items")));

        assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder(
                new JsonObject().put("name", "superman").encode(),
                new JsonObject().put("name", "wonderwoman").encode(),
                new JsonObject().put("name", "catwoman").encode());
    }

    @Test
    public void testABeanProducingHttpMessagesOfJsonObject() {
        wireMockServer.stubFor(put(urlEqualTo("/items"))
                .willReturn(aResponse()
                        .withStatus(204)));

        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        addClasses(BeanProducingHttpMessagesOfJsonObject.class, SourceBean.class);
        addConfig(new HttpConnectorConfig("http", "outgoing", getHost() + "/items"));
        initialize();

        awaitForRequest(10);
        wireMockServer.verify(10, putRequestedFor(urlEqualTo("/items")));

        bodies("/items").stream()
                .map(JsonObject::new)
                .forEach(json -> assertThat(json.getMap()).containsKey("value"));

        assertThat(bodies("/items").stream()
                .map(JsonObject::new)
                .map(j -> j.getInteger("value"))
                .collect(Collectors.toList())).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @ApplicationScoped
    public static class SourceBean {
        @Outgoing("numbers")
        public Multi<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }
    }

    @ApplicationScoped
    public static class SourceBeanWithConverter {
        @Outgoing("names")
        public Multi<String> source() {
            return Multi.createFrom().items("superman", "catwoman", "wonderwoman");
        }
    }

    @ApplicationScoped
    public static class BeanProducingJsonObjects {

        @Incoming("numbers")
        @Outgoing("http")
        public JsonObject sink(int i) {
            return new JsonObject().put("value", i + 1);
        }
    }

    @ApplicationScoped
    public static class BeanProducingMessagesOfJsonObject {

        @Incoming("numbers")
        @Outgoing("http")
        public Message<JsonObject> sink(int i) {
            return Message.of(new JsonObject().put("value", i + 1));
        }
    }

    @ApplicationScoped
    public static class BeanProducingHttpMessagesOfJsonObject {

        @Incoming("numbers")
        @Outgoing("http")
        public HttpMessage<JsonObject> sink(int i) {
            return HttpMessage.HttpMessageBuilder.<JsonObject> create()
                    .withMethod("PUT")
                    .withPayload(new JsonObject().put("value", i + 1))
                    .withHeader("Content-Type", "application/json")
                    .build();
        }
    }

    @ApplicationScoped
    public static class BeanProducingJsonArrays {

        @Incoming("numbers")
        @Outgoing("http")
        public JsonArray sink(int i) {
            return new JsonArray().add(i + 1).add(i - 1);
        }
    }

    @ApplicationScoped
    public static class BeanProducingBytes {

        @Incoming("numbers")
        @Outgoing("http")
        public byte[] sink(int i) {
            return Integer.toString(i + 1).getBytes();
        }

    }

    @ApplicationScoped
    public static class BeanProducingByteBuffers {

        @Incoming("numbers")
        @Outgoing("http")
        public ByteBuffer sink(int i) {
            return ByteBuffer.wrap(Integer.toString(i + 1).getBytes());
        }

    }

    @ApplicationScoped
    public static class BeanProducingVertxBuffers {

        @Incoming("numbers")
        @Outgoing("http")
        public Buffer sink(int i) {
            return Buffer.buffer(Integer.toString(i + 1).getBytes());
        }

    }

    @ApplicationScoped
    public static class BeanProducingPersons {

        @Incoming("names")
        @Outgoing("http")
        public Person sink(String name) {
            return new Person().setName(name);
        }

    }

}
