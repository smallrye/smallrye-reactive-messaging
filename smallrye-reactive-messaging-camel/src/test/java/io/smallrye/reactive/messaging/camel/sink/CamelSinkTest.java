package io.smallrye.reactive.messaging.camel.sink;

import io.smallrye.reactive.messaging.camel.CamelTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.awaitility.Awaitility.await;

public class CamelSinkTest extends CamelTestBase {

  private final Path path = new File("target/values.txt").toPath();

  @Before
  public void setup() throws IOException {
    Files.deleteIfExists(path);
  }

  @After
  public void tearDown() throws IOException {
    Files.deleteIfExists(path);
  }

  @Test
  public void testUsingRegularEndpointAsSink() {
    addClasses(BeanWithCamelSinkUsingRegularEndpoint.class);
    initialize();
    assertFileContent();
  }

  private void assertFileContent() {
    await().until(() -> {
      if (! path.toFile().exists()) {
        return false;
      }
      List<String> list = Files.readAllLines(path);
      return list.size() == 1 && list.get(0).equalsIgnoreCase("abcd");
    });
  }

  @Test
  public void testUsingRegularRouteAsSink() {
    addClasses(BeanWithCamelSinkUsingRegularRoute.class);
    initialize();

    assertFileContent();
  }

  @Test
  public void testUsingRSRouteAsSink() {
    addClasses(BeanWithCamelSinkUsingRSRoute.class);
    initialize();

    assertFileContent();
  }

}
