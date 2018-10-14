package io.smallrye.reactive.messaging.camel.incoming;

import io.smallrye.reactive.messaging.camel.CamelTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class IncomingCamelTest extends CamelTestBase {

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
  public void testWithABeanDeclaringACamelSink() throws IOException {
    addClasses(BeanWithCamelSink.class);
    initialize();
    BeanWithCamelSink bean = bean(BeanWithCamelSink.class);
    List<String> values = bean.values();
    await().until(() -> values.size() == 4);
    List<String> list = Files.readAllLines(path);
    assertThat(list).hasSize(1).containsExactly("abcd");
  }

  @Test
  public void testWithABeanDeclaringACamelSubscriber() {
    addClasses(BeanWithCamelSubscriber.class);
    initialize();

    await().until(() -> {
      List<String> list = Files.readAllLines(path);
      return list.size() == 1 && list.get(0).equalsIgnoreCase("abcd");
    });
  }

  @Test
  public void testWithABeanDeclaringACamelRSRouteSubscriber() {
    addClasses(BeanWithCamelSubscriberFromReactiveStreamRoute.class);
    initialize();

    await().until(() -> {
      if (! path.toFile().exists()) {
        return false;
      }
      List<String> list = Files.readAllLines(path);
      return list.size() == 1 && list.get(0).equalsIgnoreCase("abcd");
    });
  }

}
