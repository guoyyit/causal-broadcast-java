package com.github.guoyyit.causalbroadcastjava.core;

/**
 * Thrown when an injected message envelope fails identity or structure
 * validation. Rejections carry a machine-distinguishable {@link Reason} and
 * never mutate service state.
 */
public class InvalidEnvelopeException extends RuntimeException {

    /**
     * Distinguishable reasons for rejecting an injected envelope.
     */
    public enum Reason {
        /** Envelope sender never joined the broadcast group. */
        UNKNOWN_SENDER,
        /** Envelope sequence number is not positive. */
        INVALID_SEQ,
        /** Envelope id does not match its sender and sequence. */
        INVALID_ENVELOPE_ID,
        /** Causal metadata (vector clock) is missing. */
        MISSING_CLOCK,
        /** Self-declared clock entry does not match the envelope sequence. */
        CLOCK_SEQ_MISMATCH,
        /** Clock references a participant that never joined. */
        UNKNOWN_CLOCK_ENTRY
    }

    private final Reason reason;

    public InvalidEnvelopeException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
