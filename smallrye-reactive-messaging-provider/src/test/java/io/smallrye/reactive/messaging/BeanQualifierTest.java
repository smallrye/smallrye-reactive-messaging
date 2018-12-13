package io.smallrye.reactive.messaging;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.Test;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.inject.Qualifier;
import java.lang.annotation.*;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BeanQualifierTest extends WeldTestBaseWithoutTails {


  @Test
  public void testManagementOfBeanUsingAQualifier() {
    addBeanClass(BeanWithQualifier.class);
    initialize();
    BeanWithQualifier bean = container.getBeanManager().createInstance().select(BeanWithQualifier.class, Any.Literal.INSTANCE).get();
    assertThat(bean).isNotNull();
    assertThat(bean.get()).isNotEmpty().containsExactly("HELLO", "SMALLRYE", "REACTIVE", "MESSAGE");
  }

  @Foo
  @ApplicationScoped
  static class BeanWithQualifier {

    List<String> words = new ArrayList<>();

    List<String> get() {
      return words;
    }

    @Outgoing("source")
    public PublisherBuilder<String> source() {
      return ReactiveStreams.of("hello", "with", "SmallRye", "reactive", "message");
    }

    @Incoming("source")
    @Outgoing("processed-a")
    public String toUpperCase(String payload) {
      return payload.toUpperCase();
    }

    @Incoming("processed-a")
    @Outgoing("processed-b")
    public PublisherBuilder<String> filter(PublisherBuilder<String> input) {
      return input.filter(item -> item.length() > 4);
    }

    @Incoming("processed-b")
    public void sink(String word) {
      words.add(word);
    }


  }

  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public @interface Foo {}

}
