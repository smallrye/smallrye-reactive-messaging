package io.smallrye.reactive.messaging.http;

import static org.awaitility.Awaitility.await;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;

public class HttpTestBase {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);

    private Weld weld;
    private WeldContainer container;

    @Before
    public void init() {
        weld = new Weld();
    }

    @After
    public void cleanUp() {
        weld.shutdown();
    }

    public void initialize() {
        container = weld.initialize();
    }

    public HttpTestBase addClasses(Class... classes) {
        Arrays.stream(classes).forEach(weld::addBeanClass);
        return this;
    }

    public <T> T get(Class<T> c) {
        return container.getBeanManager().createInstance().select(c).get();
    }

    public void awaitForRequest(int amount) {
        await().until(() -> wireMockRule.getServeEvents().getRequests().size() == amount);
    }

    public void awaitForRequest(int amount, long timeout) {
        await()
                .atMost(timeout, TimeUnit.MILLISECONDS)
                .until(() -> wireMockRule.getServeEvents().getRequests().size() == amount);
    }

    public List<LoggedRequest> requests() {
        return wireMockRule.getServeEvents().getRequests().stream()
                .map(ServeEvent::getRequest).collect(Collectors.toList());
    }

    public List<String> bodies(String path) {
        return requests().stream().filter(req -> req.getUrl().equalsIgnoreCase(path))
                .map(LoggedRequest::getBodyAsString).collect(Collectors.toList());
    }
}
