package com.github.guoyyit.causalbroadcastjava.core;

/**
 * Machine-readable reason why an injected message envelope was rejected.
 * Surfaced to HTTP clients as the {@code reason} field of 400 responses.
 */
public enum RejectionReason {
    /** Request carried no envelope at all. */
    MISSING_ENVELOPE,
    /** Envelope has no (or blank) sender id. */
    MISSING_SENDER,
    /** Sender never joined the broadcast group. */
    UNKNOWN_SENDER,
    /** Sequence number is not a positive integer. */
    NON_POSITIVE_SEQUENCE,
    /** Envelope id is missing or not equal to {@code senderId:seq}. */
    ENVELOPE_ID_MISMATCH,
    /** Causal metadata (vector clock) is absent. */
    MISSING_CAUSAL_METADATA,
    /** Clock entry for the sender does not match the envelope sequence. */
    CLOCK_SEQ_MISMATCH,
    /** Clock references a participant that never joined. */
    UNKNOWN_CLOCK_REFERENCE
}
