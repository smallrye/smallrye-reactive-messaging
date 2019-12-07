package io.smallrye.reactive.messaging.helpers;

public class Validation {

    private Validation() {
        // avoid direct instantiation
    }

    public static <T> T notNull(T instance, String name) {
        if (isBlank(name)) {
            throw new IllegalArgumentException("`name` must be set");
        }

        if (instance == null) {
            throw new IllegalArgumentException(name + " must not be `null`");
        }

        return instance;

    }

    public static <T> T[] notEmpty(T[] array, String name) {
        if (isBlank(name)) {
            throw new IllegalArgumentException("`name` must be set");
        }

        if (array == null) {
            throw new IllegalArgumentException(name + " must not be `null`");
        }
        if (array.length == 0) {
            throw new IllegalArgumentException(name + " must not be `empty`");
        }

        return array;
    }

    public static <T> T[] noNullElements(T[] array, String name) {
        if (isBlank(name)) {
            throw new IllegalArgumentException("`name` must be set");
        }

        if (array == null) {
            throw new IllegalArgumentException(name + " must not be `null`");
        }

        for (T t : array) {
            if (t == null) {
                throw new IllegalArgumentException(name + " must not contain a `null` element");
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

    public static void isTrue(boolean mustBeTrue, String format, Object... var) {
        if (!mustBeTrue) {
            throw new IllegalArgumentException(String.format(format, var));
        }
    }
}
