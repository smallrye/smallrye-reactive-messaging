package io.smallrye.reactive.messaging.http;

import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.reactivex.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class HttpMessageTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8089);
  private Vertx vertx;
  private HttpSink sink;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    Map<String, String> map = new LinkedHashMap<>();
    map.put("url", "http://localhost:8089/items");
    sink = new HttpSink(vertx, ConfigurationHelper.create(map));
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void testHeadersAndUrlAndQuery() {
    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(404)));

    stubFor(post(urlPathMatching("/record?.*"))
      .willReturn(aResponse()
        .withStatus(204)));


    String uuid = UUID.randomUUID().toString();
    HttpMessage<String> message = HttpMessage.HttpMessageBuilder.<String>create()
      .withHeader("X-foo", "value")
      .withUrl("http://localhost:8089/record")
      .withQueryParameter("name", "clement")
      .withPayload(uuid)
      .build();

    sink.send(message);
    awaitForRequest();

    assertThat(bodies("/record?name=clement")).hasSize(1);
    LoggedRequest request = requests("/record?name=clement").get(0);
    assertThat(request.getBodyAsString()).isEqualTo(uuid);
    assertThat(request.getHeader("X-foo")).isEqualTo("value");
    assertThat(request.getMethod().getName()).isEqualToIgnoringCase("POST");
    QueryParameter name = request.getQueryParams().get("name");
    assertThat(name).isNotNull();
    assertThat(name.isSingleValued()).isTrue();
    assertThat(name.firstValue()).isEqualToIgnoringCase("clement");
  }

  @Test
  public void testWithDefaultURLWithPut() {
    stubFor(put(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(204)));

    stubFor(post(urlEqualTo("/items"))
      .willReturn(aResponse()
        .withStatus(404)));

    stubFor(post(urlPathMatching("/record?.*"))
      .willReturn(aResponse()
        .withStatus(404)));

    String uuid = UUID.randomUUID().toString();
    HttpMessage<String> message = HttpMessage.HttpMessageBuilder.<String>create()
      .withHeader("X-foo", "value")
      .withHeader("X-foo", "value-2")
      .withMethod("PUT")
      .withPayload(uuid)
      .build();

    sink.send(message);
    awaitForRequest();

    assertThat(bodies("/items")).hasSize(1);
    LoggedRequest request = requests("/items").get(0);
    assertThat(request.getBodyAsString()).isEqualTo(uuid);
    assertThat(request.getHeaders().getHeader("X-foo").values()).containsExactly("value", "value-2");
    assertThat(request.getMethod().getName()).isEqualToIgnoringCase("PUT");
  }

  private void awaitForRequest() {
    await().until(() -> wireMockRule.getServeEvents().getRequests().size() >= 1);
  }

  public List<LoggedRequest> requests() {
    return wireMockRule.getServeEvents().getRequests().stream().map(ServeEvent::getRequest).collect(Collectors.toList());
  }

  private List<LoggedRequest> requests(String path) {
    return wireMockRule.getServeEvents().getRequests().stream().map(ServeEvent::getRequest)
      .filter(req -> req.getUrl().equalsIgnoreCase(path))
      .collect(Collectors.toList());
  }

  private List<String> bodies(String path) {
    return requests(path).stream().map(LoggedRequest::getBodyAsString).collect(Collectors.toList());
  }


}
