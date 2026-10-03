package com.github.guoyyit.causalbroadcastjava.service;

import com.github.guoyyit.causalbroadcastjava.core.DeliveredMessage;
import com.github.guoyyit.causalbroadcastjava.core.Message;
import com.github.guoyyit.causalbroadcastjava.core.InvalidEnvelopeException;
import com.github.guoyyit.causalbroadcastjava.core.InvalidEnvelopeException.Reason;
import com.github.guoyyit.causalbroadcastjava.core.ParticipantState;
import com.github.guoyyit.causalbroadcastjava.core.PullResult;
import com.github.guoyyit.causalbroadcastjava.core.ReceiveResult;
import com.github.guoyyit.causalbroadcastjava.core.ReceiveStatus;
import com.github.guoyyit.causalbroadcastjava.core.ServiceState;
import com.github.guoyyit.causalbroadcastjava.persistence.StateStore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Core causal broadcast engine: membership, broadcast, causal delivery,
 * pull-based reads, metadata GC and crash recovery.
 *
 * <p>Causal delivery rule: a message m from sender s is deliverable at
 * participant p iff m.seq == delivered_p[s] + 1 and every other entry of
 * m's vector clock is already covered by p's delivery frontier. Messages
 * that fail the rule are buffered until their predecessors are delivered;
 * duplicates (already covered by the frontier or already buffered) are
 * dropped. All state transitions are persisted before returning, so a
 * restart resumes exactly from the last acknowledged state.
 */
public class CausalBroadcastService {

    private final StateStore store;
    private final boolean autoFanout;
    private final int deliveredRetention;
    private ServiceState state;

    public CausalBroadcastService(StateStore store, boolean autoFanout, int deliveredRetention) {
        this.store = store;
        this.autoFanout = autoFanout;
        this.deliveredRetention = deliveredRetention;
        this.state = store.load();
    }

    /**
     * Joins (or rejoins) the broadcast group. A (re)joining participant only
     * participates in the causal chain from this moment on: its delivery
     * frontier is initialized to the current send frontier of every known
     * participant, so it will never wait for messages broadcast before it
     * joined. Send sequence numbers stay monotonic across rejoins.
     */
    public synchronized ParticipantState join(String participantId) {
        requireId(participantId);
        ParticipantState existing = state.getParticipants().get(participantId);
        if (existing != null && existing.isActive()) {
            return existing.copy();
        }
        long sendSeq = existing != null ? existing.getSendSeq() : 0L;
        ParticipantState participant = new ParticipantState(participantId, true);
        participant.setSendSeq(sendSeq);
        for (Map.Entry<String, ParticipantState> entry : state.getParticipants().entrySet()) {
            participant.getDelivered().set(entry.getKey(), entry.getValue().getSendSeq());
        }
        participant.getDelivered().set(participantId, sendSeq);
        state.getParticipants().put(participantId, participant);
        persist();
        return participant.copy();
    }

    /**
     * Marks a participant as left. Left participants keep their final send
     * sequence (for monotonic rejoin) but no longer receive messages and no
     * longer block delivery or metadata reclamation of the remaining group.
     */
    public synchronized void leave(String participantId) {
        ParticipantState participant = requireActive(participantId);
        participant.setActive(false);
        participant.getBuffer().clear();
        gcInternal();
        persist();
    }

    /**
     * Creates a message from {@code senderId}, delivers it to the sender and,
     * when auto-fanout is enabled, to every other active participant.
     */
    public synchronized Message broadcast(String senderId, String payload) {
        ParticipantState sender = requireActive(senderId);
        long seq = sender.getSendSeq() + 1;
        Map<String, Long> clock = new TreeMap<>();
        for (Map.Entry<String, Long> entry : sender.getDelivered().entries().entrySet()) {
            if (entry.getValue() > retired(entry.getKey())) {
                clock.put(entry.getKey(), entry.getValue());
            }
        }
        clock.put(senderId, seq);
        Message message = new Message(senderId, seq, clock, payload, System.currentTimeMillis());
        sender.setSendSeq(seq);
        deliver(sender, message);
        if (autoFanout) {
            for (ParticipantState participant : state.getParticipants().values()) {
                if (participant.isActive() && !participant.getId().equals(senderId)) {
                    arriveAndDrain(participant, message);
                }
            }
        }
        gcInternal();
        persist();
        return message;
    }

    /**
     * Injects a message envelope into a receiver, modelling message arrival
     * from the network. Arrival may be out of order or duplicated; the
     * receiver buffers, deduplicates and delivers in causal order. The
     * envelope is validated before any state change: senders that never
     * joined and structurally invalid envelopes are rejected with an
     * {@link InvalidEnvelopeException} and never deliver, buffer or persist.
     */
    public synchronized ReceiveResult receive(String receiverId, Message message) {
        ParticipantState receiver = requireActive(receiverId);
        validateEnvelope(message);
        ReceiveStatus status = arrive(receiver, message);
        int delivered = status == ReceiveStatus.DELIVERED ? 1 : 0;
        if (status == ReceiveStatus.DELIVERED) {
            delivered += drainBuffer(receiver);
        }
        gcInternal();
        persist();
        return new ReceiveResult(status, delivered);
    }

    /**
     * Validates an injected envelope against group membership and its own
     * causal structure. Runs before any mutation, so a rejected request leaves
     * no trace in delivery, buffer, dedup metadata or persistent state.
     */
    private void validateEnvelope(Message message) {
        if (message == null) {
            throw new InvalidEnvelopeException(Reason.UNKNOWN_SENDER,
                    "envelope must not be null");
        }
        String senderId = message.getSenderId();
        if (senderId == null || senderId.isBlank()
                || !state.getParticipants().containsKey(senderId)) {
            throw new InvalidEnvelopeException(Reason.UNKNOWN_SENDER,
                    "envelope sender never joined: " + senderId);
        }
        if (message.getSeq() <= 0) {
            throw new InvalidEnvelopeException(Reason.INVALID_SEQ,
                    "envelope sequence number must be positive: " + message.getSeq());
        }
        String expectedId = senderId + ":" + message.getSeq();
        if (!expectedId.equals(message.getId())) {
            throw new InvalidEnvelopeException(Reason.INVALID_ENVELOPE_ID,
                    "envelope id " + message.getId() + " does not match sender/seq " + expectedId);
        }
        if (message.getClock() == null || message.getClock().isEmpty()) {
            throw new InvalidEnvelopeException(Reason.MISSING_CLOCK,
                    "envelope causal clock is missing");
        }
        Long selfSeq = message.getClock().get(senderId);
        if (selfSeq == null || selfSeq != message.getSeq()) {
            throw new InvalidEnvelopeException(Reason.CLOCK_SEQ_MISMATCH,
                    "clock self entry " + message.getClock().get(senderId)
                            + " does not match envelope seq " + message.getSeq());
        }
        for (String ref : message.getClock().keySet()) {
            if (!state.getParticipants().containsKey(ref)) {
                throw new InvalidEnvelopeException(Reason.UNKNOWN_CLOCK_ENTRY,
                        "clock references a participant that never joined: " + ref);
            }
        }
    }

    private void arriveAndDrain(ParticipantState receiver, Message message) {
        if (arrive(receiver, message) == ReceiveStatus.DELIVERED) {
            drainBuffer(receiver);
        }
    }

    /**
     * Atomically reads up to {@code limit} delivered messages after the
     * participant's read cursor and advances the persisted cursor. Concurrent
     * pulls are serialized, so no message is skipped or returned twice.
     */
    public synchronized PullResult pull(String participantId, int limit) {
        ParticipantState participant = requireParticipant(participantId);
        int cappedLimit = limit <= 0 ? 100 : Math.min(limit, 1000);
        List<DeliveredMessage> page = new ArrayList<>();
        long cursor = participant.getReadCursor();
        for (DeliveredMessage delivered : participant.getDeliveredLog()) {
            if (delivered.getDeliverySeq() > cursor && page.size() < cappedLimit) {
                page.add(delivered);
                cursor = delivered.getDeliverySeq();
            }
        }
        participant.setReadCursor(cursor);
        trimDeliveredLog(participant);
        persist();
        return new PullResult(cursor, page);
    }

    /**
     * Reclaims causal metadata that has become stable: vector-clock entries of
     * left participants whose final messages have been delivered by every
     * active participant are folded into a persistent retired floor and
     * dropped from per-participant clocks.
     */
    public synchronized void gc() {
        gcInternal();
        persist();
    }

    public synchronized List<ParticipantState> listParticipants() {
        List<ParticipantState> result = new ArrayList<>();
        for (ParticipantState participant : state.getParticipants().values()) {
            result.add(participant.copy());
        }
        return result;
    }

    public synchronized ParticipantState getParticipant(String participantId) {
        return requireParticipant(participantId).copy();
    }

    public synchronized ServiceState getState() {
        return state;
    }

    private ReceiveStatus arrive(ParticipantState receiver, Message message) {
        String senderId = message.getSenderId();
        long deliveredFromSender = effectiveDelivered(receiver, senderId);
        if (message.getSeq() <= deliveredFromSender) {
            return ReceiveStatus.DUPLICATE;
        }
        for (Message buffered : receiver.getBuffer()) {
            if (buffered.getId().equals(message.getId())) {
                return ReceiveStatus.DUPLICATE;
            }
        }
        if (!isDeliverable(receiver, message)) {
            receiver.getBuffer().add(message);
            return ReceiveStatus.BUFFERED;
        }
        deliver(receiver, message);
        return ReceiveStatus.DELIVERED;
    }

    private boolean isDeliverable(ParticipantState receiver, Message message) {
        String senderId = message.getSenderId();
        if (message.getSeq() != effectiveDelivered(receiver, senderId) + 1) {
            return false;
        }
        for (Map.Entry<String, Long> entry : message.getClock().entrySet()) {
            if (!entry.getKey().equals(senderId)
                    && effectiveDelivered(receiver, entry.getKey()) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    private void deliver(ParticipantState receiver, Message message) {
        receiver.setDeliverySeq(receiver.getDeliverySeq() + 1);
        receiver.getDeliveredLog().add(new DeliveredMessage(receiver.getDeliverySeq(), message));
        receiver.getDelivered().set(message.getSenderId(), message.getSeq());
    }

    private int drainBuffer(ParticipantState receiver) {
        int delivered = 0;
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            Iterator<Message> iterator = receiver.getBuffer().iterator();
            while (iterator.hasNext()) {
                Message buffered = iterator.next();
                if (buffered.getSeq() <= effectiveDelivered(receiver, buffered.getSenderId())) {
                    iterator.remove();
                    progressed = true;
                    continue;
                }
                if (isDeliverable(receiver, buffered)) {
                    iterator.remove();
                    deliver(receiver, buffered);
                    delivered++;
                    progressed = true;
                }
            }
        }
        return delivered;
    }

    private void gcInternal() {
        for (ParticipantState candidate : state.getParticipants().values()) {
            if (candidate.isActive()) {
                continue;
            }
            String leftId = candidate.getId();
            long finalSeq = candidate.getSendSeq();
            if (finalSeq <= retired(leftId)) {
                continue;
            }
            boolean stable = true;
            for (ParticipantState participant : state.getParticipants().values()) {
                if (participant.isActive() && effectiveDelivered(participant, leftId) < finalSeq) {
                    stable = false;
                    break;
                }
            }
            if (stable) {
                state.getRetiredClocks().merge(leftId, finalSeq, Math::max);
                for (ParticipantState participant : state.getParticipants().values()) {
                    if (participant.getDelivered().get(leftId) <= retired(leftId)) {
                        participant.getDelivered().remove(leftId);
                    }
                }
            }
        }
    }

    private void trimDeliveredLog(ParticipantState participant) {
        long floor = participant.getReadCursor() - Math.max(0, deliveredRetention);
        participant.getDeliveredLog().removeIf(entry -> entry.getDeliverySeq() <= floor);
    }

    private long effectiveDelivered(ParticipantState participant, String senderId) {
        return Math.max(participant.getDelivered().get(senderId), retired(senderId));
    }

    private long retired(String participantId) {
        return state.getRetiredClocks().getOrDefault(participantId, 0L);
    }

    private ParticipantState requireParticipant(String participantId) {
        requireId(participantId);
        ParticipantState participant = state.getParticipants().get(participantId);
        if (participant == null) {
            throw new IllegalArgumentException("unknown participant: " + participantId);
        }
        return participant;
    }

    private ParticipantState requireActive(String participantId) {
        ParticipantState participant = requireParticipant(participantId);
        if (!participant.isActive()) {
            throw new IllegalStateException("participant has left: " + participantId);
        }
        return participant;
    }

    private void requireId(String participantId) {
        if (participantId == null || participantId.isBlank()) {
            throw new IllegalArgumentException("participant id must not be blank");
        }
    }

    private void persist() {
        store.save(state);
    }
}
