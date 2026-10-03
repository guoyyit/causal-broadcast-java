package com.github.guoyyit.causalbroadcastjava.tests;

import com.github.guoyyit.causalbroadcastjava.core.DeliveredMessage;
import com.github.guoyyit.causalbroadcastjava.core.InvalidEnvelopeException;
import com.github.guoyyit.causalbroadcastjava.core.Message;
import com.github.guoyyit.causalbroadcastjava.core.PullResult;
import com.github.guoyyit.causalbroadcastjava.core.RejectionReason;
import com.github.guoyyit.causalbroadcastjava.core.ReceiveStatus;
import com.github.guoyyit.causalbroadcastjava.persistence.StateStore;
import com.github.guoyyit.causalbroadcastjava.service.CausalBroadcastService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReceiveValidationTest {

    @TempDir
    Path dataDir;

    private CausalBroadcastService newService(boolean autoFanout) {
        return new CausalBroadcastService(new StateStore(dataDir), autoFanout, 0);
    }

    private static Message envelope(String id, String senderId, long seq, Map<String, Long> clock,
                                    String payload) {
        Message message = new Message(senderId, seq, clock, payload, 0L);
        message.setId(id);
        return message;
    }

    private static Message legit(String senderId, long seq, Map<String, Long> clock, String payload) {
        return new Message(senderId, seq, clock, payload, 0L);
    }

    private static List<String> payloads(CausalBroadcastService service, String participant) {
        List<String> payloads = new ArrayList<>();
        long cursor = 0;
        while (true) {
            PullResult result = service.pull(participant, 100);
            for (DeliveredMessage delivered : result.getMessages()) {
                payloads.add(delivered.getMessage().getPayload());
            }
            if (result.getCursor() == cursor) {
                return payloads;
            }
            cursor = result.getCursor();
        }
    }

    private byte[] stateFileBytes() {
        try {
            return Files.readAllBytes(dataDir.resolve("state.json"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void assertReason(RejectionReason expected, CausalBroadcastService service,
                                     String receiver, Message message) {
        InvalidEnvelopeException thrown = assertThrows(InvalidEnvelopeException.class,
                () -> service.receive(receiver, message));
        assertEquals(expected, thrown.getReason());
    }

    private static void assertReceiverUntouched(CausalBroadcastService service, String receiver) {
        assertTrue(service.getParticipant(receiver).getBuffer().isEmpty());
        assertTrue(service.getParticipant(receiver).getDeliveredLog().isEmpty());
        assertTrue(service.getParticipant(receiver).getDelivered().entries().isEmpty());
        assertEquals(0, service.getParticipant(receiver).getDeliverySeq());
    }

    @Test
    void envelopeFromNeverJoinedSenderIsRejectedWithoutAnyStateChange() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");
        byte[] stateBefore = stateFileBytes();

        Message forged = legit("ghost", 1, Map.of("ghost", 1L), "forged");
        assertReason(RejectionReason.UNKNOWN_SENDER, service, "B", forged);

        assertReceiverUntouched(service, "B");
        assertTrue(service.getState().getRetiredClocks().isEmpty());
        assertEquals(java.util.Arrays.toString(stateBefore),
                java.util.Arrays.toString(stateFileBytes()),
                "rejection must not rewrite the persisted state");
    }

    @Test
    void forgedEnvelopeDoesNotPoisonLaterLegitimateDelivery() {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");

        Message forged = legit("mallory", 1, Map.of("mallory", 1L), "forged");
        assertReason(RejectionReason.UNKNOWN_SENDER, service, "A", forged);
        assertReason(RejectionReason.UNKNOWN_SENDER, service, "B", forged);

        service.join("mallory");
        service.broadcast("mallory", "m1");
        service.broadcast("mallory", "m2");
        service.broadcast("mallory", "m3");

        for (String receiver : List.of("A", "B", "mallory")) {
            assertEquals(List.of("m1", "m2", "m3"), payloads(service, receiver),
                    "receiver " + receiver + " must deliver exactly the legitimate messages");
        }
    }

    @Test
    void structurallyInvalidEnvelopesAreRejectedWithDistinguishableReasons() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");
        Message a1 = service.broadcast("A", "a1");
        byte[] stateBefore = stateFileBytes();

        assertReason(RejectionReason.MISSING_ENVELOPE, service, "B", null);
        assertReason(RejectionReason.MISSING_SENDER, service, "B",
                legit(null, 1, Map.of("A", 1L), "x"));
        assertReason(RejectionReason.MISSING_SENDER, service, "B",
                legit(" ", 1, Map.of("A", 1L), "x"));
        assertReason(RejectionReason.NON_POSITIVE_SEQUENCE, service, "B",
                legit("A", 0, Map.of("A", 0L), "x"));
        assertReason(RejectionReason.NON_POSITIVE_SEQUENCE, service, "B",
                legit("A", -3, Map.of("A", -3L), "x"));
        assertReason(RejectionReason.ENVELOPE_ID_MISMATCH, service, "B",
                envelope("A:99", "A", 1, Map.of("A", 1L), "x"));
        assertReason(RejectionReason.ENVELOPE_ID_MISMATCH, service, "B",
                envelope("B:1", "A", 1, Map.of("A", 1L), "x"));
        assertReason(RejectionReason.ENVELOPE_ID_MISMATCH, service, "B",
                envelope(null, "A", 1, Map.of("A", 1L), "x"));
        Message noClock = legit("A", 1, Map.of("A", 1L), "x");
        noClock.setClock(null);
        assertReason(RejectionReason.MISSING_CAUSAL_METADATA, service, "B", noClock);
        assertReason(RejectionReason.CLOCK_SEQ_MISMATCH, service, "B",
                legit("A", 1, Map.of("A", 2L), "x"));
        assertReason(RejectionReason.CLOCK_SEQ_MISMATCH, service, "B",
                legit("A", 1, Map.of(), "x"));
        assertReason(RejectionReason.UNKNOWN_CLOCK_REFERENCE, service, "B",
                legit("A", 1, new TreeMap<>(Map.of("A", 1L, "ghost", 4L)), "x"));

        assertReceiverUntouched(service, "B");
        assertEquals(java.util.Arrays.toString(stateBefore),
                java.util.Arrays.toString(stateFileBytes()),
                "rejections must not rewrite the persisted state");

        assertEquals(ReceiveStatus.DELIVERED, service.receive("B", a1).getStatus());
        assertEquals(ReceiveStatus.DUPLICATE, service.receive("B", a1).getStatus());
        assertEquals(List.of("a1"), payloads(service, "B"));
    }

    @Test
    void inFlightMessageFromLeftParticipantIsStillAccepted() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");
        Message b1 = service.broadcast("B", "b1");
        service.leave("B");

        assertEquals(ReceiveStatus.DELIVERED, service.receive("A", b1).getStatus());
        assertEquals(List.of("b1"), payloads(service, "A"));
    }
}
