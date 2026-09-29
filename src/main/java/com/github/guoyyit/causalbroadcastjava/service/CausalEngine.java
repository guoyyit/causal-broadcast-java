package com.github.guoyyit.causalbroadcastjava.service;

import com.github.guoyyit.causalbroadcastjava.config.CausalProperties;
import com.github.guoyyit.causalbroadcastjava.model.BroadcastMessage;
import com.github.guoyyit.causalbroadcastjava.model.MemberKey;
import com.github.guoyyit.causalbroadcastjava.model.VectorClock;
import com.github.guoyyit.causalbroadcastjava.store.DurableStore;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * Core causal-broadcast engine.
 *
 * <p>State is fully server-authoritative and persisted through
 * {@link DurableStore}; a single JVM-wide lock serializes every mutation and
 * pull, which makes delivery, cursor advancement and metadata reclamation
 * restart-safe and free of read/write races.
 */
@Service
public class CausalEngine {

    private final DurableStore store;
    private final CausalProperties properties;
    private final ReentrantLock lock = new ReentrantLock();

    public CausalEngine(DurableStore store, CausalProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    @PostConstruct
    void onStartup() {
        lock.lock();
        try {
            reclaimStableMetadata();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Registers (or re-registers) a participant. A returning participant gets a
     * fresh generation, so it can only participate in causal chains from the
     * join point; its baseline clock snapshots global stability.
     */
    public MemberKey join(String memberId) {
        requireMemberId(memberId);
        lock.lock();
        try {
            int generation = store.maxGeneration(memberId) + 1;
            MemberKey memberKey = new MemberKey(memberId, generation);
            VectorClock baseline = globalActiveClock();
            String previous = previousComponent(memberId);
            if (previous != null) {
                baseline = baseline.withoutComponent(previous);
            }
            store.insertMember(memberId, memberKey.key(),
                    store.writeClock(baseline.entries()));
            reclaimStableMetadata();
            return memberKey;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes a participant. Everything still buffered anywhere is first
     * flushed to a delivery fixed point, so the departed member can never keep
     * buffering or metadata reclamation blocked.
     */
    public void leave(String memberId) {
        lock.lock();
        try {
            MemberKey memberKey = requireActiveMember(memberId);
            store.markAllReceived();
            runDeliveryFixedPoint();
            store.deactivateMember(memberId);
            pruneComponentEverywhere(memberKey.key());
            reclaimStableMetadata();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Broadcasts one message from the sender. The sender's causal clock is
     * server-authoritative; the message is delivered to the sender immediately.
     */
    public BroadcastMessage broadcast(String senderId, String payload) {
        if (payload == null) {
            throw new ResponseStatusException(BAD_REQUEST, "payload must not be null");
        }
        lock.lock();
        try {
            MemberKey senderKey = requireActiveMember(senderId);
            VectorClock senderClock = currentClock(senderId);
            VectorClock messageClock = senderClock.incremented(senderKey.key());
            long seq = messageClock.get(senderKey.key());

            List<String> targetKeys = store.listActiveMembers().stream()
                    .map(CausalEngine::componentKey)
                    .toList();

            BroadcastMessage message = new BroadcastMessage(
                    UUID.randomUUID().toString(),
                    senderKey.key(),
                    senderId,
                    seq,
                    store.writeClock(messageClock.entries()),
                    payload,
                    List.copyOf(targetKeys));
            store.insertMessage(message, targetKeys);
            store.updateMemberClock(senderId, store.writeClock(messageClock.entries()));
            long deliverSeq = store.nextDeliverSeq(senderKey.key());
            store.markDelivered(senderKey.key(), message.id(), deliverSeq);
            runDeliveryFixedPoint();
            reclaimStableMetadata();
            return store.findMessage(message.id());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Delivers an already-broadcast message to a receiver. Idempotent:
     * duplicate deliveries, retries and early calls while predecessors are
     * buffered all converge to exactly-once delivery.
     */
    public void receive(String receiverId, String messageId) {
        lock.lock();
        try {
            MemberKey receiverKey = requireActiveMember(receiverId);
            BroadcastMessage message = store.findMessage(messageId);
            if (message == null) {
                throw new ResponseStatusException(NOT_FOUND, "unknown message id");
            }
            int updated = store.markReceived(receiverKey.key(), messageId);
            if (updated > 0) {
                runDeliveryFixedPoint();
            }
            reclaimStableMetadata();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Pulls delivered messages strictly after {@code afterSeq} and returns the
     * cursor to continue with on the next pull.
     */
    public PullResult pull(String memberId, Long afterSeq, int limit) {
        if (afterSeq != null && afterSeq < 0) {
            throw new ResponseStatusException(BAD_REQUEST, "afterSeq must not be negative");
        }
        if (limit <= 0 || limit > properties.getPullMaxLimit()) {
            throw new ResponseStatusException(BAD_REQUEST,
                    "limit must be within 1 and " + properties.getPullMaxLimit());
        }
        long cursor = afterSeq == null ? 0L : afterSeq;
        lock.lock();
        try {
            requireMember(memberId);
            String memberKey = store.findMemberKey(memberId);
            List<DurableStore.DeliveredRecord> page =
                    store.deliveredPage(memberKey, cursor, limit);
            List<BroadcastMessage> messages = page.stream()
                    .map(DurableStore.DeliveredRecord::message)
                    .toList();
            long nextSeq = page.isEmpty() ? cursor : page.get(page.size() - 1).deliverSeq();
            return new PullResult(messages, nextSeq);
        } finally {
            lock.unlock();
        }
    }

    /** Test/admin helper: wipes all local state. */
    public void reset() {
        lock.lock();
        try {
            store.deleteAllData();
        } finally {
            lock.unlock();
        }
    }

    private void runDeliveryFixedPoint() {
        boolean progressed;
        do {
            progressed = false;
            for (Map<String, Object> member : store.listActiveMembers()) {
                String memberId = (String) member.get("id");
                String memberKey = componentKey(member);
                VectorClock clock = currentClock(memberId);
                for (BroadcastMessage message : store.findPendingMessages(memberKey)) {
                    if (isDeliverable(message, clock)) {
                        long deliverSeq = store.nextDeliverSeq(memberKey);
                        store.markDelivered(memberKey, message.id(), deliverSeq);
                        clock = clock.mergedWith(store.readClock(message.clockJson()));
                        store.updateMemberClock(memberId, store.writeClock(clock.entries()));
                        progressed = true;
                    }
                }
            }
        } while (progressed);
    }

    private boolean isDeliverable(BroadcastMessage message, VectorClock clock) {
        Map<String, Long> required = store.readClock(message.clockJson());
        for (Map.Entry<String, Long> entry : required.entrySet()) {
            String component = entry.getKey();
            long needed = entry.getValue();
            long have = clock.get(component);
            if (component.equals(message.senderKey())) {
                if (have != needed - 1) {
                    return false;
                }
            } else if (have < needed) {
                return false;
            }
        }
        return true;
    }

    private void pruneComponentEverywhere(String componentKey) {
        for (Map<String, Object> member : store.listActiveMembers()) {
            String memberId = (String) member.get("id");
            VectorClock pruned = currentClock(memberId).withoutComponent(componentKey);
            store.updateMemberClock(memberId, store.writeClock(pruned.entries()));
        }
    }

    private void reclaimStableMetadata() {
        while (true) {
            List<String> stable = store.candidateStableMessages(properties.getGcBatchSize());
            if (stable.isEmpty()) {
                return;
            }
            store.nullStableMessageClocks(stable);
        }
    }

    private VectorClock globalActiveClock() {
        VectorClock merged = new VectorClock();
        for (Map<String, Object> member : store.listActiveMembers()) {
            merged = merged.mergedWith(store.readClock((String) member.get("clock_json")));
        }
        return merged;
    }

    private String previousComponent(String memberId) {
        int generation = store.maxGeneration(memberId);
        return generation == 0 ? null : memberId + "#" + generation;
    }

    private VectorClock currentClock(String memberId) {
        Map<String, Object> row = store.findMemberRow(memberId);
        if (row == null) {
            throw new ResponseStatusException(NOT_FOUND, "unknown participant: " + memberId);
        }
        return new VectorClock(store.readClock((String) row.get("clock_json")));
    }

    private void requireMember(String memberId) {
        if (!store.memberExists(memberId)) {
            throw new ResponseStatusException(NOT_FOUND, "unknown participant: " + memberId);
        }
    }

    private MemberKey requireActiveMember(String memberId) {
        requireMemberId(memberId);
        Map<String, Object> row = store.findMemberRow(memberId);
        if (row == null || !Boolean.TRUE.equals(row.get("active"))) {
            throw new ResponseStatusException(CONFLICT,
                    "participant is not active: " + memberId);
        }
        return new MemberKey(memberId, ((Number) row.get("gen")).intValue());
    }

    private static String componentKey(Map<String, Object> memberRow) {
        return memberRow.get("id") + "#" + ((Number) memberRow.get("gen")).intValue();
    }

    private static void requireMemberId(String memberId) {
        if (memberId == null || memberId.isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "member id must not be blank");
        }
    }
}
