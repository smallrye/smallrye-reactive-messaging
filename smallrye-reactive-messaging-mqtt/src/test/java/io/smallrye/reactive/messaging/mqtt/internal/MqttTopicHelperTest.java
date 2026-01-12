package io.smallrye.reactive.messaging.mqtt.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@DisplayName("MQTT Topic Validation, Filter and Matcher Tests")
class MqttTopicHelperTest {

    @Nested
    @DisplayName("Topic Validation Tests")
    class ValidationTests {

        @Test
        public void validationTest() {

            MqttTopicHelper.validateTopicFilter("#");
            MqttTopicHelper.validateTopicFilter("+");
            MqttTopicHelper.validateTopicFilter("/");
            MqttTopicHelper.validateTopicFilter("home/temperature");
            MqttTopicHelper.validateTopicFilter("home/+/temperature");
            MqttTopicHelper.validateTopicFilter("home/#");
            MqttTopicHelper.validateTopicFilter("home/+/+");
            MqttTopicHelper.validateTopicFilter("home//+/+");
            MqttTopicHelper.validateTopicFilter("/home//+/+");
            MqttTopicHelper.validateTopicFilter("/home");
            MqttTopicHelper.validateTopicFilter("home//temp");
            MqttTopicHelper.validateTopicFilter("///");
            MqttTopicHelper.validateTopicFilter("$share/name//");
            MqttTopicHelper.validateTopicFilter("$share/name/+/");
            MqttTopicHelper.validateTopicFilter("$share/name/#");

            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter(""));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("home/#/temperature"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("home/temp#"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("home/temperature+"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("home/temperature+/sens"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("home/+temperature"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("home/+temperature/sens"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("$share/+"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("$share/+/sdf"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("$share/+/sdf"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("$share/#"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("$share/#/"));
            assertThrows(IllegalArgumentException.class, () -> MqttTopicHelper.validateTopicFilter("$share/#/sdf"));
        }

    }

    @Nested
    @DisplayName("Topic Matching Tests")
    class MatchingTests {

        @Nested
        @DisplayName("Single-Level Wildcard (+) Matching")
        class SingleLevelWildcardMatching {

            @ParameterizedTest
            @CsvSource({
                    "sport/tennis/+, sport/tennis/player1, true",
                    "sport/tennis/+, sport/tennis/player2, true",
                    "sport/tennis/+, sport/tennis/player1/ranking, false",
                    "+/+, home/kitchen, true",
                    "+/+, home/kitchen/light, false",
                    "home/+/temperature, home/kitchen/temperature, true",
                    "home/+/temperature, home/bedroom/temperature, true",
                    "home/+/temperature, home/kitchen/bedroom/temperature, false"
            })
            @DisplayName("Should correctly match single-level wildcards")
            void shouldMatchSingleLevelWildcard(String filter, String topic, boolean expected) {
                assertEquals(expected, MqttTopicHelper.matches(filter, topic));
            }

            @Test
            @DisplayName("Single-level wildcard should match empty level")
            void shouldMatchEmptyLevel() {
                assertTrue(MqttTopicHelper.matches("home/+/temperature", "home//temperature"));
            }
        }

        @Nested
        @DisplayName("Multi-Level Wildcard (#) Matching")
        class MultiLevelWildcardMatching {

            @ParameterizedTest
            @CsvSource({
                    "sport/tennis/player1/#, sport/tennis/player1, true",
                    "sport/tennis/player1/#, sport/tennis/player1/ranking, true",
                    "sport/tennis/player1/#, sport/tennis/player1/score/wimbledon, true",
                    "sport/#, sport, true",
                    "home/#, home, true",
                    "home/#, homes, false",
                    "home/#, home/kitchen, true",
                    "home/#, home/kitchen/temperature, true",
                    "#, home/kitchen/temperature, true",
                    "#, any/topic/at/all, true"
            })
            @DisplayName("Should correctly match multi-level wildcards")
            void shouldMatchMultiLevelWildcard(String filter, String topic, boolean expected) {
                assertEquals(expected, MqttTopicHelper.matches(filter, topic));
            }
        }

        @Nested
        @DisplayName("Literal and Empty level Topic Matching")
        class LiteralTopicMatching {

            @ParameterizedTest
            @CsvSource({
                    "home/kitchen, home/kitchen, true",
                    "home/kitchen, home/bedroom, false",
                    "home/kitchen/temperature, home/kitchen, false",
                    "home/kitchen, home/kitchen/temperature, false",
                    "//, //, true",
                    "home//temperature, home//temperature, true"
            })
            @DisplayName("Should correctly match literal topics")
            void shouldMatchLiteralTopics(String filter, String topic, boolean expected) {
                assertEquals(expected, MqttTopicHelper.matches(filter, topic));
            }
        }

        @Nested
        @DisplayName("System Topics ($ prefix) Matching")
        class SystemTopicMatching {

            @Test
            @DisplayName("System topics should not match # wildcard at start")
            void systemTopicsShouldNotMatchHashAtStart() {
                assertFalse(MqttTopicHelper.matches("#", "$SYS/broker/clients"));
            }

            @Test
            @DisplayName("System topics should not match + wildcard at start")
            void systemTopicsShouldNotMatchPlusAtStart() {
                assertFalse(MqttTopicHelper.matches("+/monitor/Clients", "$SYS/monitor/Clients"));
            }

            @Test
            @DisplayName("System topics should match when filter explicitly starts with $")
            void systemTopicsShouldMatchExplicitFilter() {
                assertTrue(MqttTopicHelper.matches("$SYS/#", "$SYS/broker/clients"));
                assertTrue(MqttTopicHelper.matches("$SYS/+/clients", "$SYS/broker/clients"));
            }
        }

        @Nested
        @DisplayName("Shared Subscription Matching")
        class SharedSubscriptionMatching {

            @ParameterizedTest
            @CsvSource({
                    "$share/group1/home/+/temp, home/kitchen/temp, true",
                    "$share/consumer1/#, any/topic, true",
                    "$share/group/sensor/+, sensor/temperature, true",
                    "$share/group/home/kitchen, home/kitchen, true",
                    "$share/group/home/kitchen, home/bedroom, false"
            })
            @DisplayName("Should extract and match filter from shared subscriptions")
            void shouldMatchSharedSubscriptions(String filter, String topic, boolean expected) {
                assertEquals(expected, MqttTopicHelper.matches(filter, topic));
            }

            @Test
            @DisplayName("Shared subscription with # should not match $ topics")
            void sharedSubscriptionHashShouldNotMatchSystemTopics() {
                assertFalse(MqttTopicHelper.matches("$share/group/#", "$SYS/broker"));
            }
        }

        @Nested
        @DisplayName("Edge Cases")
        class EdgeCases {

            @Test
            @DisplayName("Should handle null topic name")
            void shouldHandleNullTopicName() {
                assertFalse(MqttTopicHelper.matches("home/+", null));
            }

            @Test
            @DisplayName("Should match empty levels correctly")
            void shouldMatchEmptyLevels() {
                assertTrue(MqttTopicHelper.matches("///", "///"));
                assertTrue(MqttTopicHelper.matches("+/+/+", "//"));
                assertFalse(MqttTopicHelper.matches("+/+/+", "///"));
            }

            @Test
            @DisplayName("Should handle special regex characters in topic name")
            void shouldHandleRegexSpecialChars() {
                assertTrue(MqttTopicHelper.matches("home/temp.sensor", "home/temp.sensor"));
                assertTrue(MqttTopicHelper.matches("home/temp[1]", "home/temp[1]"));
                assertTrue(MqttTopicHelper.matches("home/temp(a)", "home/temp(a)"));
            }
        }
    }

    @Nested
    @DisplayName("Pattern Conversion Tests")
    class PatternConversionTests {

        @Test
        @DisplayName("Pattern should correctly use regex quantifiers")
        void shouldContainCorrectRegexQuantifiers() {
            Pattern pattern = MqttTopicHelper.topicFilterToPattern("home/+/temp").pattern;
            String regex = pattern.pattern();
            // Check that it contains the single-level wildcard pattern
            assertTrue(regex.contains("[^/]*"));
            // Check that it starts with ^ and ends with $
            assertTrue(regex.startsWith("^"));
            assertTrue(regex.endsWith("$"));
        }

        @Test
        @DisplayName("Pattern with # should use .* for multi-level matching")
        void shouldUseCorrectMultiLevelWildcard() {
            Pattern pattern = MqttTopicHelper.topicFilterToPattern("home/#").pattern;
            String regex = pattern.pattern();
            // Check that it contains the multi-level wildcard pattern
            assertTrue(regex.contains(".*"));
        }
    }

}