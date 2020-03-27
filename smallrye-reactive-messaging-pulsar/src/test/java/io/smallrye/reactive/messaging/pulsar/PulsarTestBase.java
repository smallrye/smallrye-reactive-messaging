package io.smallrye.reactive.messaging.pulsar;

import io.vertx.mutiny.core.Vertx;
import org.junit.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;



public class PulsarTestBase {

    private static final Integer BROKER_PORT = 6650;
    private static final Integer BROKER_HTTP_PORT = 8080;
    private static final String METRICS_ENDPOINT = "/metrics";

    @ClassRule
    public static GenericContainer pulsarContainer = new GenericContainer("apachepulsar/pulsar-standalone:2.5.0")
        .withExposedPorts(BROKER_PORT, BROKER_HTTP_PORT)
        .withCommand("/pulsar/bin/pulsar", "standalone", "--no-functions-worker", "-nss")
        .waitingFor(Wait.forHttp(METRICS_ENDPOINT)
        .forStatusCode(200)
        .forPort(BROKER_HTTP_PORT));

    Vertx vertx;
    String address;
    Integer port;


    @Test
    public void testContainer(){
        Assert.assertNotNull(pulsarContainer);
    }

    @Before
    public void setup() {

    }

    @After
    public void tearDown() throws InterruptedException {

    }

}
