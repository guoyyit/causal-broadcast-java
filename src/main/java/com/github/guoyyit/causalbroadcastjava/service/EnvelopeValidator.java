package com.github.guoyyit.causalbroadcastjava.service;

import com.github.guoyyit.causalbroadcastjava.core.InvalidEnvelopeException;
import com.github.guoyyit.causalbroadcastjava.core.Message;
import com.github.guoyyit.causalbroadcastjava.core.RejectionReason;
import com.github.guoyyit.causalbroadcastjava.core.ServiceState;

/**
 * Validates injected message envelopes before they may touch any state.
 * An envelope is accepted only if its sender is a known participant
 * (active or left, but never fabricated) and its structure is internally
 * consistent: positive sequence, id equal to {@code senderId:seq}, a
 * vector clock whose self-entry matches the sequence and whose referenced
 * participants are all known. Violations raise
 * {@link InvalidEnvelopeException} with a distinguishable reason and leave
 * both the in-memory and the persisted state untouched.
 */
final class EnvelopeValidator {

    private EnvelopeValidator() {
    }

    static void validate(Message message, ServiceState state) {
        if (message == null) {
            throw new InvalidEnvelopeException(RejectionReason.MISSING_ENVELOPE,
                    "request body must contain a message envelope");
        }
        String senderId = message.getSenderId();
        if (senderId == null || senderId.isBlank()) {
            throw new InvalidEnvelopeException(RejectionReason.MISSING_SENDER,
                    "envelope senderId must not be blank");
        }
        if (message.getSeq() <= 0) {
            throw new InvalidEnvelopeException(RejectionReason.NON_POSITIVE_SEQUENCE,
                    "envelope seq must be a positive integer, got " + message.getSeq());
        }
        String expectedId = senderId + ":" + message.getSeq();
        if (!expectedId.equals(message.getId())) {
            throw new InvalidEnvelopeException(RejectionReason.ENVELOPE_ID_MISMATCH,
                    "envelope id '" + message.getId() + "' does not match expected '" + expectedId + "'");
        }
        if (message.getClock() == null) {
            throw new InvalidEnvelopeException(RejectionReason.MISSING_CAUSAL_METADATA,
                    "envelope must carry its causal metadata (vector clock)");
        }
        if (!isKnown(senderId, state)) {
            throw new InvalidEnvelopeException(RejectionReason.UNKNOWN_SENDER,
                    "sender '" + senderId + "' never joined the broadcast group");
        }
        Long selfSeq = message.getClock().get(senderId);
        if (selfSeq == null || selfSeq != message.getSeq()) {
            throw new InvalidEnvelopeException(RejectionReason.CLOCK_SEQ_MISMATCH,
                    "clock entry for sender '" + senderId + "' is " + selfSeq
                            + " but envelope seq is " + message.getSeq());
        }
        for (String referenced : message.getClock().keySet()) {
            if (!isKnown(referenced, state)) {
                throw new InvalidEnvelopeException(RejectionReason.UNKNOWN_CLOCK_REFERENCE,
                        "clock references participant '" + referenced + "' which never joined");
            }
        }
    }

    private static boolean isKnown(String participantId, ServiceState state) {
        return state.getParticipants().containsKey(participantId)
                || state.getRetiredClocks().containsKey(participantId);
    }
}
