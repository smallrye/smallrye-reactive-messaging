package io.smallrye.reactive.messaging.http;

import com.github.tomakehurst.wiremock.http.Fault;
import io.reactivex.Flowable;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;


public class HttpSinkTest extends HttpTestBase {

  @Test
  public void testABeanProducingJsonObjects() {
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingJsonObjects.class, SourceBean.class);
    initialize();

    awaitForRequest(10);
    verify(10, postRequestedFor(urlEqualTo("/items")));

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
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingMessagesOfJsonObject.class, SourceBean.class);
    initialize();

    awaitForRequest(10);
    verify(10, postRequestedFor(urlEqualTo("/items")));

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
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withLogNormalRandomDelay(90, 0.1)
        .withStatus(204)));

    addClasses(BeanProducingJsonObjects.class, SourceBean.class);
    initialize();

    awaitForRequest(10, 60000);
    verify(10, postRequestedFor(urlEqualTo("/items")));

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
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(500)));

    addClasses(BeanProducingJsonObjects.class, SourceBean.class);
    initialize();

    awaitForRequest(1);
    verify(1, postRequestedFor(urlEqualTo("/items")));

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
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

    addClasses(BeanProducingJsonObjects.class, SourceBean.class);
    initialize();

    awaitForRequest(1);
    verify(1, postRequestedFor(urlEqualTo("/items")));

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
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingJsonArrays.class, SourceBean.class);
    initialize();

    awaitForRequest(10);
    verify(10, postRequestedFor(urlEqualTo("/items")));

    bodies("/items").stream()
      .map(JsonArray::new)
      .forEach(json -> assertThat(json).hasSize(2));
  }

  @Test
  public void testABeanProducingBytes() {
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingBytes.class, SourceBean.class);
    initialize();

    awaitForRequest(10);
    verify(10, postRequestedFor(urlEqualTo("/items")));

    assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
  }

  @Test
  public void testABeanProducingByteBuffers() {
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingByteBuffers.class, SourceBean.class);
    initialize();

    awaitForRequest(10);
    verify(10, postRequestedFor(urlEqualTo("/items")));

    assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
  }

  @Test
  public void testABeanProducingVertxBuffers() {
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingVertxBuffers.class, SourceBean.class);
    initialize();

    awaitForRequest(10);
    verify(10, postRequestedFor(urlEqualTo("/items")));

    assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
  }

  @Test
  public void testABeanUsingCustomCodec() {
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingPersons.class, SourceBeanWithConverter.class);

    initialize();

    awaitForRequest(3);
    verify(3, postRequestedFor(urlEqualTo("/items")));

    assertThat(new ArrayList<>(bodies("/items"))).containsExactlyInAnyOrder(
      new JsonObject().put("name", "superman").encode(),
      new JsonObject().put("name", "wonderwoman").encode(),
      new JsonObject().put("name", "catwoman").encode()
    );
  }

  @Test
  public void testABeanProducingHttpMessagesOfJsonObject() {
    stubFor(put(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    addClasses(BeanProducingHttpMessagesOfJsonObject.class, SourceBean.class);
    initialize();

    awaitForRequest(10);
    verify(10, putRequestedFor(urlEqualTo("/items")));

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
    public Flowable<Integer> source() {
      return Flowable.range(0, 10);
    }

    @Produces
    public Config config() {
      return new HttpSinkConfig("http", "sink","http://localhost:8089/items");
    }
  }

  @ApplicationScoped
  public static class SourceBeanWithConverter {
    @Outgoing("names")
    public Flowable<String> source() {
      return Flowable.fromArray("superman", "catwoman", "wonderwoman");
    }

    @Produces
    public Config config() {
      return new HttpSinkConfig("http", "sink","http://localhost:8089/items").converter(PersonSerializer.class.getName());
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
      return HttpMessage.HttpMessageBuilder.<JsonObject>create()
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
