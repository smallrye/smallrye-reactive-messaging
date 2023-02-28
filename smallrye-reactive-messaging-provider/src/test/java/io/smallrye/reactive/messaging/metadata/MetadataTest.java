package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

public class MetadataTest {

    @Test
    public void testEmptyMetadata() {
        Metadata metadata = Metadata.empty();
        assertThat(metadata).hasSize(0);
    }

    @Test
    public void testCreationFromEmpty() {
        Metadata metadata = Metadata.of();
        assertThat(metadata).hasSize(0);

        Metadata metadata2 = metadata.with(new MessageTest.MyMetadata<>("bar"));
        assertThat(metadata2).hasSize(1);
        assertThat(metadata2).allSatisfy(o -> assertThat(o).isInstanceOf(MessageTest.MyMetadata.class));
        assertThat(metadata).isNotSameAs(metadata2);
    }

    @Test
    public void testOf() {
        Metadata h1 = Metadata.of();
        assertThat(h1).isEmpty();

        MessageTest.MyMetadata<Integer> m = new MessageTest.MyMetadata<>(12);
        Metadata h2 = Metadata.of(m);
        assertThat(h2).hasSize(1).containsExactly(m);

        AtomicBoolean m2 = new AtomicBoolean(true);
        Metadata h3 = Metadata.of(m, m2);
        assertThat(h3).hasSize(2).containsExactlyInAnyOrder(m, m2);

        MessageTest.MyMetadata<Double> m3 = new MessageTest.MyMetadata<>(23.3);
        assertThatThrownBy(() -> Metadata.of(m, m2, m3)).isInstanceOf(IllegalArgumentException.class);
        Metadata h4 = Metadata.of(m).with(m2).with(m3);
        assertThat(h4).hasSize(2).containsExactlyInAnyOrder(m2, m3);
    }

    @Test
    public void testCreationWithOneEntry() {
        Metadata metadata = Metadata.of(new MessageTest.MyMetadata<>("bar"));
        assertThat(metadata).hasSize(1);
    }

    @Test
    public void cannotCreateFromNull() {
        assertThatThrownBy(
                () -> Metadata.of((Object) null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                () -> Metadata.of(new MessageTest.MyMetadata<>("a"), null, "hello"))
                .isInstanceOf(IllegalArgumentException.class);

        // Test with duplicated keys
        assertThatThrownBy(() -> Metadata
                .of(new MessageTest.MyMetadata<>("a"),
                        "hello",
                        new MessageTest.MyMetadata<>("b")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWithout() {
        MessageTest.MyMetadata<String> myMetadata = new MessageTest.MyMetadata<>("hello");
        Metadata metadata = Metadata.empty().with(myMetadata);
        assertThat(metadata).hasSize(1).containsExactlyInAnyOrder(myMetadata);
        Metadata without = metadata.without(MessageTest.MyMetadata.class);
        assertThat(metadata).isNotSameAs(without);
        assertThat(without).isEmpty();

        Metadata metadata2 = metadata.with("world").without(String.class);
        assertThat(metadata2).hasSize(1).containsExactlyInAnyOrder(myMetadata);
        Metadata metadata3 = metadata.with("world").without(MessageTest.MyMetadata.class);
        assertThat(metadata3).hasSize(1).containsExactlyInAnyOrder("world");
    }

    @Test
    public void testGetObject() {
        Person person = new Person();
        person.name = "mark";
        byte[] bytes = new byte[] { 1, 2, 3, 4 };

        Metadata metadata = Metadata.of(person, bytes);
        Message<String> message = Message.of("ignored", metadata);

        Person p = message.getMetadata(Person.class).orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(p).isNotNull();
        assertThat(p.name).isEqualTo("mark");

        byte[] b = message.getMetadata(byte[].class).orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(b).isNotNull().hasSize(4).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void testFrom() {
        int v = 1;
        String s = "hello";

        Metadata metadata = Metadata.empty().with(v);
        Metadata copy = Metadata.from(metadata).with(s).with(2);
        assertThat(copy).hasSize(2).containsExactlyInAnyOrder(s, 2);
        assertThat(metadata).hasSize(1).containsExactlyInAnyOrder(1);
    }

    private static class Person {
        String name;
    }

}
