package com.github.guoyyit.causalbroadcastjava.core;

/**
 * Thrown when an injected message envelope fails sender-identity or
 * structural validation. Carries a machine-readable rejection reason;
 * the HTTP layer maps it to 400. A rejected envelope never causes any
 * state transition or persistence write.
 */
public class InvalidEnvelopeException extends RuntimeException {

    private final RejectionReason reason;

    public InvalidEnvelopeException(RejectionReason reason, String detail) {
        super(detail);
        this.reason = reason;
    }

    public RejectionReason getReason() {
        return reason;
    }
}
