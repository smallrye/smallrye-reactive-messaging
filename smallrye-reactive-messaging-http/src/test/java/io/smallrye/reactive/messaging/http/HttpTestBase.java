package io.smallrye.reactive.messaging.http;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;

import io.smallrye.config.SmallRyeConfigProviderResolver;

public class HttpTestBase {

    private Weld weld;
    private WeldContainer container;
    static WireMockServer wireMockServer;

    @BeforeAll
    public static void startServer() {
        wireMockServer = new WireMockServer(new WireMockConfiguration().dynamicPort());
        wireMockServer.start();
    }

    @AfterAll
    public static void stopServer() {
        wireMockServer.stop();
    }

    @AfterEach
    public void afterEach() {
        wireMockServer.resetAll();
    }

    static String getHost() {
        return "http://localhost:" + wireMockServer.port();
    }

    @BeforeEach
    public void init() {
        weld = new Weld();
    }

    @AfterEach
    public void cleanUp() {
        weld.shutdown();
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    public void initialize() {
        container = weld.initialize();
    }

    public void addClasses(Class<?>... classes) {
        Arrays.stream(classes).forEach(weld::addBeanClass);
    }

    public static void addConfig(HttpConnectorConfig config) {
        if (config != null) {
            config.write();
        } else {
            clear();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void clear() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
    }

    public <T> T get(Class<T> c) {
        return container.getBeanManager().createInstance().select(c).get();
    }

    public void awaitForRequest(int amount) {
        await().until(() -> wireMockServer.getServeEvents().getRequests().size() == amount);
    }

    public void awaitForRequest(int amount, long timeout) {
        await()
                .atMost(timeout, TimeUnit.MILLISECONDS)
                .until(() -> wireMockServer.getServeEvents().getRequests().size() == amount);
    }

    public List<LoggedRequest> requests() {
        return wireMockServer.getServeEvents().getRequests().stream()
                .map(ServeEvent::getRequest).collect(Collectors.toList());
    }

    public List<String> bodies(String path) {
        return requests().stream().filter(req -> req.getUrl().equalsIgnoreCase(path))
                .map(LoggedRequest::getBodyAsString).collect(Collectors.toList());
    }
}
