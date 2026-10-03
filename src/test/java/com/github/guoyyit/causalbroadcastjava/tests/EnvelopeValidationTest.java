package com.github.guoyyit.causalbroadcastjava.tests;

import com.github.guoyyit.causalbroadcastjava.core.InvalidEnvelopeException;
import com.github.guoyyit.causalbroadcastjava.core.InvalidEnvelopeException.Reason;
import com.github.guoyyit.causalbroadcastjava.core.Message;
import com.github.guoyyit.causalbroadcastjava.core.ParticipantState;
import com.github.guoyyit.causalbroadcastjava.core.PullResult;
import com.github.guoyyit.causalbroadcastjava.core.ReceiveResult;
import com.github.guoyyit.causalbroadcastjava.core.ReceiveStatus;
import com.github.guoyyit.causalbroadcastjava.persistence.StateStore;
import com.github.guoyyit.causalbroadcastjava.service.CausalBroadcastService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Envelope identity and structure validation: forged senders and malformed
 * envelopes are rejected with a distinguishable reason without touching
 * delivery, buffer, dedup metadata or persistent state.
 */
class EnvelopeValidationTest {

    @TempDir
    Path dataDir;

    private CausalBroadcastService newService(boolean autoFanout) {
        return new CausalBroadcastService(new StateStore(dataDir), autoFanout, 0);
    }

    private static List<String> payloads(CausalBroadcastService service, String participant) {
        List<String> payloads = new ArrayList<>();
        long cursor = 0;
        while (true) {
            PullResult result = service.pull(participant, 100);
            result.getMessages().forEach(m -> payloads.add(m.getMessage().getPayload()));
            if (result.getCursor() == cursor) {
                return payloads;
            }
            cursor = result.getCursor();
        }
    }

    private static Message envelope(String sender, long seq, String id, Map<String, Long> clock) {
        Message message = new Message(sender, seq, clock == null ? Map.of() : clock, "payload", 0L);
        message.setId(id);
        if (clock == null) {
            message.setClock(null);
        }
        return message;
    }

    private void assertNoStateChange(CausalBroadcastService service, String participant) {
        ParticipantState state = service.getParticipant(participant);
        assertTrue(state.getBuffer().isEmpty(), "rejected envelope must not enter the buffer");
        assertTrue(state.getDeliveredLog().isEmpty(), "rejected envelope must not be delivered");
    }

    private InvalidEnvelopeException expectRejected(CausalBroadcastService service,
                                                    String receiver, Message envelope,
                                                    Reason reason) {
        InvalidEnvelopeException error = assertThrows(InvalidEnvelopeException.class,
                () -> service.receive(receiver, envelope));
        assertEquals(reason, error.getReason());
        return error;
    }

    @Test
    void envelopeFromUnknownSenderIsRejectedAndNeverAffectsLaterJoin() {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");

        Message forged = envelope("ghost", 1, "ghost:1", Map.of("ghost", 1L));
        expectRejected(service, "A", forged, Reason.UNKNOWN_SENDER);
        assertNoStateChange(service, "A");
        assertNoStateChange(service, "B");
        assertFalse(service.getState().getParticipants().containsKey("ghost"));

        service.join("ghost");
        for (int i = 1; i <= 3; i++) {
            service.broadcast("ghost", "g" + i);
        }

        List<String> deliveredByA = payloads(service, "A");
        List<String> deliveredByB = payloads(service, "B");
        assertEquals(List.of("g1", "g2", "g3"), deliveredByA);
        assertEquals(List.of("g1", "g2", "g3"), deliveredByB);
        assertFalse(deliveredByA.contains("forged"));
        assertFalse(deliveredByB.contains("forged"));
    }

    @Test
    void blankSenderIsRejectedAsUnknownSender() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        expectRejected(service, "A", envelope("", 1, ":1", Map.of()), Reason.UNKNOWN_SENDER);
        assertNoStateChange(service, "A");
    }

    @Test
    void nonPositiveSequenceIsRejected() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        expectRejected(service, "A", envelope("A", 0, "A:0", Map.of("A", 0L)),
                Reason.INVALID_SEQ);
        expectRejected(service, "A", envelope("A", -1, "A:-1", Map.of("A", -1L)),
                Reason.INVALID_SEQ);
        assertNoStateChange(service, "A");
    }

    @Test
    void envelopeIdMismatchIsRejected() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        expectRejected(service, "A", envelope("A", 1, "A:99", Map.of("A", 1L)),
                Reason.INVALID_ENVELOPE_ID);
        expectRejected(service, "A", envelope("A", 1, "B:1", Map.of("A", 1L)),
                Reason.INVALID_ENVELOPE_ID);
        assertNoStateChange(service, "A");
    }

    @Test
    void missingClockIsRejected() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        expectRejected(service, "A", envelope("A", 1, "A:1", null), Reason.MISSING_CLOCK);
        expectRejected(service, "A", envelope("A", 1, "A:1", Map.of()), Reason.MISSING_CLOCK);
        assertNoStateChange(service, "A");
    }

    @Test
    void clockSelfEntryMismatchIsRejected() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        expectRejected(service, "A", envelope("A", 1, "A:1", Map.of("A", 2L)),
                Reason.CLOCK_SEQ_MISMATCH);
        assertNoStateChange(service, "A");
    }

    @Test
    void clockReferencingUnknownSenderIsRejected() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.broadcast("A", "a1");
        long sendSeqBefore = service.getParticipant("A").getSendSeq();
        int deliveredBefore = service.getParticipant("A").getDeliveredLog().size();

        expectRejected(service, "A", envelope("A", 2, "A:2", Map.of("A", 2L, "ghost", 1L)),
                Reason.UNKNOWN_CLOCK_ENTRY);
        assertTrue(service.getParticipant("A").getBuffer().isEmpty());
        assertEquals(deliveredBefore, service.getParticipant("A").getDeliveredLog().size());
        assertEquals(sendSeqBefore, service.getParticipant("A").getSendSeq());
    }

    @Test
    void rejectedEnvelopeDoesNotCorruptLaterDedup() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");

        expectRejected(service, "B", envelope("ghost", 1, "ghost:1", Map.of("ghost", 1L)),
                Reason.UNKNOWN_SENDER);
        expectRejected(service, "B", envelope("A", 1, "A:99", Map.of("A", 1L)),
                Reason.INVALID_ENVELOPE_ID);

        Message a1 = service.broadcast("A", "a1");
        ReceiveResult first = service.receive("B", a1);
        assertEquals(ReceiveStatus.DELIVERED, first.getStatus());
        ReceiveResult duplicate = service.receive("B", a1);
        assertEquals(ReceiveStatus.DUPLICATE, duplicate.getStatus());
        assertEquals(List.of("a1"), payloads(service, "B"));
    }
}
