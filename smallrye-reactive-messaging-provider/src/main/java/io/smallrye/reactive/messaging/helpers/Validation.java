package io.smallrye.reactive.messaging.helpers;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

public class Validation {

    private Validation() {
        // avoid direct instantiation
    }

    public static <T> T notNull(T instance, String name) {
        if (isBlank(name)) {
            throw ex.nameMustBeSet();
        }

        if (instance == null) {
            throw ex.validationForNotNull(name);
        }

        return instance;

    }

    public static <T> T[] notEmpty(T[] array, String name) {
        if (isBlank(name)) {
            throw ex.nameMustBeSet();
        }

        if (array == null) {
            throw ex.validationForNotNull(name);
        }
        if (array.length == 0) {
            throw ex.validationForNotEmpty(name);
        }

        return array;
    }

    public static <T> T[] noNullElements(T[] array, String name) {
        if (isBlank(name)) {
            throw ex.nameMustBeSet();
        }

        if (array == null) {
            throw ex.validationForNotNull(name);
        }

        for (T t : array) {
            if (t == null) {
                throw ex.validationForContainsNull(name);
            }
        }

        return array;
    }

    /**
     * <p>
     * Checks if a given {@code string} is blank or {@code null}.
     *
     * @param string the String to check, may be {@code null}
     * @return {@code true} if the CharSequence is null, empty or whitespace only
     */
    public static boolean isBlank(String string) {
        int strLen;
        if (string == null || (strLen = string.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(string.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static void isTrue(boolean mustBeTrue, String message) {
        if (!mustBeTrue) {
            throw ex.validateIsTrue(message);
        }
    }
}
