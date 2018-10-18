package io.smallrye.reactive.messaging.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Configure the acknowledgement policy for the given {@code @Incoming}.
 *
 * Check <a href="https://docs.google.com/spreadsheets/d/11cNwX8eXKTKjoF_amXTPLU1-DfrYxwu4VLnEideBy7Q/edit?usp=drive_web&ouid=116267359728910593612">this spreadsheet</a>
 * to check the default policy for a given signature and the supported mode.
 *
 * Temporary annotation - must be copied to spec.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Acknowledgment {

  enum Mode {
    /**
     * No acknowledgment performed
     */
    NONE,
    /**
     * Acknowledgment managed by the user code. No automatic acknowledgment is performed.
     */
    MANUAL,
    /**
     * Acknowledgment performed automatically before the user processing of the message.
     */
    PRE_PROCESSING,

    /**
     * Acknowledgment performed automatically after the user processing of the message. Notice that this mode is not supported for all signatures.
     * When supported it's the default policy.
     *
     * Check <a href="https://docs.google.com/spreadsheets/d/11cNwX8eXKTKjoF_amXTPLU1-DfrYxwu4VLnEideBy7Q/edit?usp=drive_web&ouid=116267359728910593612">this spreadsheet</a>
     * for details.
     */
    POST_PROCESSING
  }

  Mode value();

}
