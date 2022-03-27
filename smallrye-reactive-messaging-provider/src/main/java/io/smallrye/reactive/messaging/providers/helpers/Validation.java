package io.smallrye.reactive.messaging.providers.helpers;

public class Validation {

    private Validation() {
        // avoid direct instantiation
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
}
