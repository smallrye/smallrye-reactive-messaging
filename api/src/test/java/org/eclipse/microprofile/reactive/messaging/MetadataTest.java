package org.eclipse.microprofile.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

class MetadataTest {

    @Test
    void testCreation() {
        Metadata metadata = Metadata.of(new Meta1("val"), new Meta2(1));
        assertThat(metadata.get(Meta1.class).map(m -> m.value)).hasValue("val");
        assertThat(metadata.get(Meta2.class).map(m -> m.count)).hasValue(1);
        assertThat(metadata.get(List.class)).isEmpty();

        assertThat(metadata).hasSize(2)
                .anyMatch(o -> o instanceof Meta1)
                .anyMatch(o -> o instanceof Meta2);

        assertThat(metadata.with(new Meta3(2))).hasSize(3);
        assertThat(metadata.without(Meta1.class)).hasSize(1);
        assertThat(metadata.without(Meta3.class)).hasSize(2);

        assertThatThrownBy(() -> metadata.without(null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> metadata.get(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCreationWithSingleObject() {
        Metadata metadata = Metadata.of(new Meta1("val"));
        assertThat(metadata.get(Meta1.class).map(m -> m.value)).hasValue("val");
        assertThat(metadata.get(Meta2.class)).isEmpty();

        assertThat(metadata).hasSize(1)
                .anyMatch(o -> o instanceof Meta1);

        assertThat(metadata.with(new Meta3(2))).hasSize(2);
        assertThat(metadata.without(Meta1.class)).hasSize(0);
        assertThat(metadata.without(Meta3.class)).hasSize(1);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void testCreationFromNull() {
        assertThatThrownBy(() -> Metadata.of((Meta1) null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> Metadata.of((Object[]) null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> Metadata.of(new Meta1("x"), null))
                .isInstanceOf(IllegalArgumentException.class);

        Metadata metadata = Metadata.from(Collections.singleton(new Meta1("z")));
        assertThatThrownBy(() -> metadata.with(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvalidCreation() {
        assertThatThrownBy(() -> Metadata.of(new Meta1("x"), new Meta1("y")))
                .isInstanceOf(IllegalArgumentException.class);

        // However, the following is accepted:
        Metadata metadata = Metadata.of(new Meta1("x"));
        assertThat(metadata.with(new Meta1("y")).get(Meta1.class).map(m -> m.value))
                .hasValue("y");
    }

    @Test
    void testEmpty() {
        Metadata metadata = Metadata.empty();
        assertThat(metadata.iterator()).isExhausted();
        assertThat(metadata.get(Meta1.class)).isEmpty();
        assertThat(metadata.with(new Meta1("test")).get(Meta1.class).map(m -> m.value)).hasValue("test");
        assertThat(metadata.without(Meta2.class).get(Meta1.class)).isEmpty();
        assertThat(metadata.copy().iterator()).isExhausted();

        // Creating from an empty iterable is equivalent to empty
        assertThat(Metadata.from(Collections.emptyList())).isEqualTo(metadata);
    }

    static class Meta1 {
        final String value;

        public Meta1(String value) {
            this.value = value;
        }
    }

    static class Meta2 {
        final int count;

        public Meta2(int count) {
            this.count = count;
        }
    }

    static class Meta3 {
        final int count;

        public Meta3(int count) {
            this.count = count;
        }
    }

}
