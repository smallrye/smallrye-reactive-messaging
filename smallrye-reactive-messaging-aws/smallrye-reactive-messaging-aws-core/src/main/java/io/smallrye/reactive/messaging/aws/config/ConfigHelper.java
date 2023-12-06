package io.smallrye.reactive.messaging.aws.config;

import java.util.*;

public class ConfigHelper {

    public static List<String> parseToList(String valueToParse) {
        List<String> result = new ArrayList<>();

        Arrays.stream(valueToParse.split(","))
                .map(String::trim)
                .forEach(result::add);

        return result;
    }

    public static Map<String, String> parseToMap(String valueToParse) {
        Map<String, String> result = new HashMap<>();

        Arrays.stream(valueToParse.split(","))
                .map(String::trim)
                .forEach(keyValue -> {
                    String[] keyValueSplit = keyValue.split(":");
                    if (keyValueSplit.length == 2) {
                        String key = keyValueSplit[0];
                        String value = keyValueSplit[1];
                        result.put(key, value);
                    }
                });

        return result;
    }
}
