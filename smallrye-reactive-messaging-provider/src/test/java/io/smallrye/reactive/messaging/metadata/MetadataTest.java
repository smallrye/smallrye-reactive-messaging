package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.*;

import java.util.*;

import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.Test;

public class MetadataTest {

    @Test
    public void testEmptyMetadata() {
        Metadata metadata = Metadata.empty();
        assertThat(metadata).hasSize(0)
                .doesNotContainKeys("foo")
                .doesNotContainValue("bar");

        assertThat(metadata.getAsBoolean("missing")).isFalse();
        assertThat(metadata.getAsBoolean("missing", true)).isTrue();
        assertThat(metadata.getAsInteger("missing", 23)).isEqualTo(23);
        assertThat(metadata.getAsLong("missing", 23L)).isEqualTo(23L);
        assertThat(metadata.getAsString("missing", "hello")).isEqualTo("hello");
        assertThat(metadata.getAsDouble("missing", 23.3)).isEqualTo(23.3);
        assertThat(metadata.containsValue(null)).isFalse();
    }

    @Test
    public void testCreationFromEmpty() {
        Metadata metadata = Metadata.of();
        assertThat(metadata).hasSize(0)
                .doesNotContainKeys("foo")
                .doesNotContainValue("bar");

        Metadata metadata2 = metadata.with("foo", "bar");
        assertThat(metadata2).hasSize(1)
                .containsKey("foo")
                .containsValue("bar");
        assertThat(metadata).isNotSameAs(metadata2);
    }

    @Test
    public void testOf() {
        Metadata h1 = Metadata.of();
        assertThat(h1).isEmpty();

        Metadata h2 = Metadata.of("a", 12);
        assertThat(h2).hasSize(1).containsKey("a").containsValue(12);
        assertThat(h2.getAsInteger("a", -1)).isEqualTo(12);

        Metadata h3 = Metadata.of("a", 12, "b", true);
        assertThat(h3).hasSize(2).containsKeys("a", "b").containsValues(12, true);
        assertThat(h3.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h3.getAsBoolean("b")).isTrue();

        Metadata h4 = Metadata.of("a", 12, "b", true, "c", 23.3);
        assertThat(h4).hasSize(3).containsKeys("a", "b", "c").containsValues(12, true, 23.3);
        assertThat(h4.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h4.getAsBoolean("b")).isTrue();
        assertThat(h4.getAsDouble("c", -1)).isEqualTo(23.3);

        Metadata h5 = Metadata.of("a", 12, "b", true, "c", 23.3, "d", Long.MAX_VALUE);
        assertThat(h5).hasSize(4).containsKeys("a", "b", "c", "d").containsValues(12, true, 23.3, Long.MAX_VALUE);
        assertThat(h5.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h5.getAsBoolean("b")).isTrue();
        assertThat(h5.getAsDouble("c", -1)).isEqualTo(23.3);
        assertThat(h5.getAsLong("d", -1)).isEqualTo(Long.MAX_VALUE);

        Metadata h6 = Metadata.of("a", 12, "b", true, "c", 23.3, "d", Long.MAX_VALUE, "e", "hello");
        assertThat(h6).hasSize(5).containsKeys("a", "b", "c", "d").containsValues(12, true, 23.3, Long.MAX_VALUE);
        assertThat(h6.getAsInteger("a", -1)).isEqualTo(12);
        assertThat(h6.getAsBoolean("b")).isTrue();
        assertThat(h6.getAsDouble("c", -1)).isEqualTo(23.3);
        assertThat(h6.getAsLong("d", -1)).isEqualTo(Long.MAX_VALUE);
        assertThat(h6.getAsString("e", null)).isEqualTo("hello");
    }

    @Test
    public void testCreationWithOneEntry() {
        Metadata metadata = Metadata.of("foo", "bar");
        assertThat(metadata).hasSize(1)
                .containsKey("foo")
                .containsValue("bar");

        assertThat(metadata.getAsString("foo", null)).isEqualTo("bar");
        assertThat(metadata.containsKey("foo")).isTrue();
        assertThat(metadata.containsValue("bar")).isTrue();
        List<Object> values = new ArrayList<>();
        metadata.forEach((k, v) -> values.add(v));
        assertThat(values).containsExactly("bar");
        assertThat(metadata.containsValue(null)).isFalse();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void cannotCreateFromNull() {
        assertThatThrownBy(() -> Metadata.of(null, "value")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Metadata.of("key", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Metadata.of("k", "v", null, "v")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Metadata.of("k", "v").containsKey(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testContainsWithNull() {
        Metadata metadata = Metadata.of("a", "b");
        assertThatThrownBy(() -> metadata.containsKey(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> metadata.containsKey((Object) null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetWithNull() {
        Metadata metadata = Metadata.of("a", "b");
        assertThatThrownBy(() -> metadata.get(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWithout() {
        Metadata metadata = Metadata.empty().with("hello", "world");
        assertThat(metadata).hasSize(1);
        Metadata without = metadata.without("hello");
        assertThat(metadata).isNotSameAs(without);
        assertThat(without).isEmpty();
    }

    @Test
    public void testGetObject() {
        Person person = new Person();
        person.name = "mark";
        Metadata metadata = Metadata.of("k1", person, "k2", new byte[] { 1, 2, 3, 4 });

        Person p = metadata.get("k1", new Person());
        assertThat(p).isNotNull();
        assertThat(p.name).isEqualTo("mark");

        byte[] bytes = metadata.get("k2", new byte[0]);
        assertThat(bytes).isNotNull().hasSize(4).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void testFrom() {
        Metadata metadata = Metadata.empty().with("hello", "world");
        Metadata copy = Metadata.from(metadata).with("k", "v").with("hello", "monde").build();
        assertThat(copy).hasSize(2).contains(entry("k", "v"), entry("hello", "monde"));
        assertThat(metadata).hasSize(1).containsExactly(entry("hello", "world"));
    }

    @Test
    public void testCopy() {
        Person person = new Person();
        person.name = "mark";

        Metadata metadata = Metadata.builder()
                .with("foo", 1234)
                .with("person", person).build();

        assertThat(metadata).hasSize(2);
        assertThat(metadata.get("person", Person.class).name).isEqualTo("mark");

        Metadata copy = metadata.copy();
        assertThat(copy).hasSize(2);
        assertThat(copy.get("person", Person.class).name).isEqualTo("mark");
    }

    @Test
    public void testImmutability() {
        Metadata metadata = Metadata.of("k", "v");
        assertThat(metadata).hasSize(1);

        // Values
        Collection<Object> collection = metadata.values();
        assertThat(collection).hasSize(1).containsExactly("v");
        assertThatThrownBy(() -> collection.add("something")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(metadata).hasSize(1).doesNotContainValue("something");

        // Keys
        Set<String> keys = metadata.keySet();
        assertThat(keys).hasSize(1).containsExactly("k");
        assertThatThrownBy(() -> keys.add("something")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(metadata).hasSize(1).doesNotContainKey("something");

        // Entry
        Set<Map.Entry<String, Object>> entries = metadata.entrySet();
        assertThat(entries).hasSize(1);
        entries.add(entry("foo", "bar"));
        assertThat(metadata).hasSize(1).doesNotContainKey("foo");

        // Entry modification
        assertThatThrownBy(() -> {
            Map.Entry<String, Object> next = entries.iterator().next();
            next.setValue("foo");
        }).isInstanceOf(UnsupportedOperationException.class);
        assertThat(metadata).hasSize(1).doesNotContainValue("foo").containsExactly(entry("k", "v"));

        // Modification methods
        assertThatThrownBy(() -> metadata.put("foo", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.remove("foo")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.remove("foo", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.putAll(Collections.emptyMap())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.putIfAbsent("foo", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.compute("foo", (s, o) -> o)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.computeIfPresent("foo", (s, o) -> o))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.computeIfAbsent("foo", s -> "bat")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.replace("foo", "bat", "bar")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.replaceAll((s, o) -> o)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.merge("foo", "bar", (s, o) -> o)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(metadata::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testBuilderWithDuplicateKeys() {
        assertThatThrownBy(() -> Metadata.of("a", 1, "a", 2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testConversionFromString() {
        Metadata metadata = Metadata.builder()
                .with("int", "1234")
                .with("long", "11111111")
                .with("boolean-t", "true")
                .with("boolean-f", "false")
                .with("double", "23.3")
                .with("person", new Person())
                .build();

        assertThat(metadata.getAsInteger("int", -1)).isEqualTo(1234);
        assertThat(metadata.getAsLong("long", -1)).isEqualTo(11111111L);
        assertThat(metadata.getAsBoolean("boolean-t")).isTrue();
        assertThat(metadata.getAsBoolean("boolean-f")).isFalse();
        assertThat(metadata.getAsDouble("double", -1)).isEqualTo(23.3);
        assertThat(metadata.getAsBoolean("int")).isFalse();

        assertThatThrownBy(() -> metadata.getAsInteger("boolean-t", -1)).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> metadata.getAsLong("boolean-t", -1)).isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> metadata.getAsDouble("boolean-t", -1)).isInstanceOf(NumberFormatException.class);

        assertThatThrownBy(() -> metadata.getAsBoolean("person")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> metadata.getAsInteger("person", -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> metadata.getAsLong("person", -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> metadata.getAsDouble("person", -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> metadata.get("person", String.class)).isInstanceOf(ClassCastException.class);

        assertThat(metadata.getAsString("person", null)).isNotNull().contains("Person");
        assertThat(metadata.get("missing", new Person())).isNotNull();
    }

    @Test
    public void testGetWithTarget() {
        Person p1 = new Person();
        p1.name = "mark";
        Metadata metadata = Metadata.of("person", p1);
        assertThatThrownBy(() -> metadata.get("person", null)).isInstanceOf(IllegalArgumentException.class);
        assertThat(metadata.get("missing", Person.class)).isNull();
        assertThat(metadata.get("person", Person.class)).isNotNull().isEqualTo(p1);
    }

    private class Person {
        String name;
    }

}
