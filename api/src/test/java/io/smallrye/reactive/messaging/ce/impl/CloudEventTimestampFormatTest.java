package io.smallrye.reactive.messaging.ce.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CloudEventTimestampFormatTest {

    DateTimeFormatter customFormatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendZoneOrOffsetId()
            .toFormatter();
    DateTimeFormatter isoOffsetFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    static Stream<Arguments> testFormatIsoAndParseCustom_args() {
        return Stream.of(
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 6789123, ZoneOffset.ofHours(-7)),
                        "2023-01-02T03:04:05.006789123-07:00"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 370582000, ZoneOffset.ofHours(-7)),
                        "2023-01-02T03:04:05.370582-07:00"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 0, ZoneOffset.ofHours(-7)), "2023-01-02T03:04:05-07:00"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 6789123, ZoneOffset.UTC), "2023-01-02T03:04:05.006789123Z"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 370582000, ZoneOffset.UTC), "2023-01-02T03:04:05.370582Z"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC), "2023-01-02T03:04:05Z"));
    }

    @ParameterizedTest
    @MethodSource("testFormatIsoAndParseCustom_args")
    void testFormatIsoAndParseCustom(ZonedDateTime instant, String formatted) {
        String format = isoOffsetFormatter.format(instant);
        assertThat(format).isEqualTo(formatted);
        ZonedDateTime time = ZonedDateTime.parse(format, customFormatter);
        assertThat(instant).isEqualTo(time);
    }

    static Stream<Arguments> testFormatCustomAndParseIso_args() {
        return Stream.of(
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 6789123, ZoneOffset.ofHours(-7)),
                        "2023-01-02T03:04:05.006789123-07:00"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 370582000, ZoneOffset.ofHours(-7)),
                        "2023-01-02T03:04:05.370582-07:00"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 0, ZoneOffset.ofHours(-7)), "2023-01-02T03:04:05-07:00"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 6789123, ZoneOffset.UTC), "2023-01-02T03:04:05.006789123Z"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 370582000, ZoneOffset.UTC), "2023-01-02T03:04:05.370582Z"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC), "2023-01-02T03:04:05Z"));
    }

    @ParameterizedTest
    @MethodSource("testFormatCustomAndParseIso_args")
    void testFormatCustomAndParseIso(ZonedDateTime instant, String formatted) {
        String format = customFormatter.format(instant);
        assertThat(format).isEqualTo(formatted);
        ZonedDateTime time = ZonedDateTime.parse(format, isoOffsetFormatter);
        assertThat(instant).isEqualTo(time);
    }

    static Stream<Arguments> testFormatCustomAndParseIsoFailed_args() {
        return Stream.of(
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 6789123, ZoneId.of("GMT")),
                        "2023-01-02T03:04:05.006789123GMT"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 370582000, ZoneId.of("GMT")),
                        "2023-01-02T03:04:05.370582GMT"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 0, ZoneId.of("GMT")), "2023-01-02T03:04:05GMT"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 6789123, ZoneId.of("Europe/Paris")),
                        "2023-01-02T03:04:05.006789123Europe/Paris"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 370582000, ZoneId.of("Europe/Paris")),
                        "2023-01-02T03:04:05.370582Europe/Paris"),
                Arguments.of(ZonedDateTime.of(2023, 1, 2, 3, 4, 5, 0, ZoneId.of("Europe/Paris")),
                        "2023-01-02T03:04:05Europe/Paris"));
    }

    @ParameterizedTest
    @MethodSource("testFormatCustomAndParseIsoFailed_args")
    void testFormatCustomAndParseIsoFailed(ZonedDateTime instant, String formatted) {
        String format = customFormatter.format(instant);
        assertThat(format).isEqualTo(formatted);
        assertThatThrownBy(() -> ZonedDateTime.parse(format, isoOffsetFormatter))
                .isInstanceOf(DateTimeParseException.class);
    }

    static Stream<Arguments> testParseCustomAndFormatIso_args() {
        return Stream.of(
                Arguments.of("2023-01-25T19:07:06.370582GMT", "2023-01-25T19:07:06.370582Z"),
                Arguments.of("2023-01-25T19:07:06.370582Z", "2023-01-25T19:07:06.370582Z"),
                Arguments.of("2023-01-25T19:07:06.370582Europe/Paris", "2023-01-25T19:07:06.370582+01:00"),
                Arguments.of("2023-01-25T19:07:06.006789123-07:00", "2023-01-25T19:07:06.006789123-07:00"),
                Arguments.of("2023-01-25T19:07:06Z", "2023-01-25T19:07:06Z"),
                Arguments.of("2023-01-25T19:07:06Europe/Paris", "2023-01-25T19:07:06+01:00"),
                Arguments.of("2023-01-25T19:07:06+02:00", "2023-01-25T19:07:06+02:00"));
    }

    @ParameterizedTest
    @MethodSource("testParseCustomAndFormatIso_args")
    void testParseCustomAndFormatIso(String formatted, String isoParsed) {
        ZonedDateTime parsed = ZonedDateTime.parse(formatted, customFormatter);
        String format = isoOffsetFormatter.format(parsed);
        assertThat(format).isEqualTo(isoParsed);
    }

    static Stream<Arguments> testParseIsoAndFormatCustom_args() {
        return Stream.of(
                Arguments.of("2023-01-25T19:07:06.370582Z", "2023-01-25T19:07:06.370582Z"),
                Arguments.of("2023-01-25T19:07:06Z", "2023-01-25T19:07:06Z"),
                Arguments.of("2023-01-02T03:04:05.006789123-07:00", "2023-01-02T03:04:05.006789123-07:00"),
                Arguments.of("2023-01-02T03:04:05-07:00", "2023-01-02T03:04:05-07:00"));
    }

    @ParameterizedTest
    @MethodSource("testParseIsoAndFormatCustom_args")
    void testParseIsoAndFormatCustom(String formatted, String isoParsed) {
        ZonedDateTime parsed = ZonedDateTime.parse(formatted, isoOffsetFormatter);
        String format = customFormatter.format(parsed);
        assertThat(format).isEqualTo(isoParsed);
    }

    static Stream<Arguments> testParseIsoFailed_args() {
        return Stream.of(
                Arguments.of("2023-01-25T19:07:06.370582GMT"),
                Arguments.of("2023-01-25T19:07:06GMT"));
    }

    @ParameterizedTest
    @MethodSource("testParseIsoFailed_args")
    void testParseIsoFailed(String formatted) {
        assertThatThrownBy(() -> ZonedDateTime.parse(formatted, isoOffsetFormatter))
                .isInstanceOf(DateTimeParseException.class);
    }

}
