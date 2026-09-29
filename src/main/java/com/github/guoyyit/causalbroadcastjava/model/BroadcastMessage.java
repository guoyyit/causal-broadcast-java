package com.github.guoyyit.causalbroadcastjava.model;

import java.util.List;

/**
 * A single broadcast message with its causal metadata.
 *
 * @param id        globally unique message id
 * @param senderKey vector-clock component key of the sender incarnation
 * @param senderId  participant id of the sender
 * @param seq       sender component value of this message
 * @param clockJson causal metadata (JSON vector clock), null once reclaimed
 * @param payload   application payload
 * @param targets   participant ids this message is addressed to
 */
public record BroadcastMessage(
        String id,
        String senderKey,
        String senderId,
        long seq,
        String clockJson,
        String payload,
        List<String> targets) {
}
