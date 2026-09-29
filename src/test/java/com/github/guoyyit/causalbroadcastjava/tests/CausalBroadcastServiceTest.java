package com.github.guoyyit.causalbroadcastjava.tests;

import com.github.guoyyit.causalbroadcastjava.core.DeliveredMessage;
import com.github.guoyyit.causalbroadcastjava.core.Message;
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
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalBroadcastServiceTest {

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
            for (DeliveredMessage delivered : result.getMessages()) {
                payloads.add(delivered.getMessage().getPayload());
            }
            if (result.getCursor() == cursor) {
                return payloads;
            }
            cursor = result.getCursor();
        }
    }

    @Test
    void buffersOutOfOrderMessagesUntilPredecessorsDelivered() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");
        service.join("C");

        Message a1 = service.broadcast("A", "a1");
        assertEquals(ReceiveStatus.DELIVERED, service.receive("B", a1).getStatus());
        Message b1 = service.broadcast("B", "b1");

        ReceiveResult buffered = service.receive("C", b1);
        assertEquals(ReceiveStatus.BUFFERED, buffered.getStatus());
        assertEquals(0, buffered.getDeliveredCount());
        assertTrue(payloads(service, "C").isEmpty());

        ReceiveResult unblocked = service.receive("C", a1);
        assertEquals(ReceiveStatus.DELIVERED, unblocked.getStatus());
        assertEquals(2, unblocked.getDeliveredCount());
        assertEquals(List.of("a1", "b1"), payloads(service, "C"));
    }

    @Test
    void duplicateDeliveryIsIgnored() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");

        Message a1 = service.broadcast("A", "a1");
        Message a2 = service.broadcast("A", "a2");

        assertEquals(ReceiveStatus.BUFFERED, service.receive("B", a2).getStatus());
        assertEquals(ReceiveStatus.DUPLICATE, service.receive("B", a2).getStatus());
        assertEquals(ReceiveStatus.DELIVERED, service.receive("B", a1).getStatus());
        assertEquals(ReceiveStatus.DUPLICATE, service.receive("B", a1).getStatus());
        assertEquals(ReceiveStatus.DUPLICATE, service.receive("B", a2).getStatus());
        assertEquals(List.of("a1", "a2"), payloads(service, "B"));
    }

    @Test
    void causalChainsAreOrderedEverywhereWhileConcurrentMayDiffer() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");
        service.join("C");
        service.join("D");

        Message a1 = service.broadcast("A", "a1");
        Message b1 = service.broadcast("B", "b1");
        assertEquals(ReceiveStatus.DELIVERED, service.receive("B", a1).getStatus());
        Message b2 = service.broadcast("B", "b2");

        for (String receiver : List.of("C", "D")) {
            assertEquals(ReceiveStatus.BUFFERED, service.receive(receiver, b2).getStatus());
            service.receive(receiver, b1);
            service.receive(receiver, a1);
        }

        for (String receiver : List.of("C", "D")) {
            List<String> delivered = payloads(service, receiver);
            assertTrue(delivered.containsAll(List.of("a1", "b1", "b2")));
            assertTrue(delivered.indexOf("a1") < delivered.indexOf("b2"),
                    "a1 must be delivered before its causal dependent b2");
        }
    }

    @Test
    void lateJoinerOnlySeesMessagesAfterJoinAndLeaveDoesNotBlockOthers() {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");
        service.broadcast("A", "before-join");

        service.join("C");
        assertTrue(payloads(service, "C").isEmpty());

        service.broadcast("A", "after-join");
        assertEquals(List.of("after-join"), payloads(service, "C"));

        service.leave("B");
        service.broadcast("A", "after-leave");
        assertEquals(List.of("before-join", "after-join", "after-leave"), payloads(service, "A"));
        assertEquals(List.of("after-leave"), payloads(service, "C"));
        assertThrows(IllegalStateException.class, () -> service.broadcast("B", "x"));
        assertThrows(IllegalArgumentException.class, () -> service.broadcast("ghost", "x"));
    }

    @Test
    void rejoinResumesWithMonotonicSequenceAndFreshFrontier() {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");
        service.broadcast("B", "b1");
        service.leave("B");
        service.broadcast("A", "a1");

        service.join("B");
        assertTrue(payloads(service, "B").isEmpty());
        Message b2 = service.broadcast("B", "b2");
        assertEquals(2, b2.getSeq());
        assertEquals(List.of("b1", "a1", "b2"), payloads(service, "A"));
    }

    @Test
    void restartRecoversBufferCursorAndDeliveredState() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");
        Message a1 = service.broadcast("A", "a1");
        Message a2 = service.broadcast("A", "a2");
        Message a3 = service.broadcast("A", "a3");

        assertEquals(ReceiveStatus.BUFFERED, service.receive("B", a3).getStatus());
        assertEquals(ReceiveStatus.DELIVERED, service.receive("B", a1).getStatus());
        PullResult firstPage = service.pull("B", 1);
        assertEquals(List.of("a1"),
                firstPage.getMessages().stream().map(m -> m.getMessage().getPayload()).toList());

        CausalBroadcastService recovered = newService(false);
        assertEquals(1, recovered.getParticipant("B").getBuffer().size());
        assertEquals(1, recovered.getParticipant("B").getReadCursor());

        ReceiveResult result = recovered.receive("B", a2);
        assertEquals(ReceiveStatus.DELIVERED, result.getStatus());
        assertEquals(2, result.getDeliveredCount());
        assertEquals(List.of("a2", "a3"), payloads(recovered, "B"));
        assertEquals(ReceiveStatus.DUPLICATE, recovered.receive("B", a1).getStatus());
    }

    @Test
    void gcRetiresStableClocksOfLeftParticipantsAndKeepsDeduplication() {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");
        service.join("C");
        service.broadcast("B", "b1");
        service.broadcast("B", "b2");
        service.leave("B");
        service.gc();

        assertEquals(2L, service.getState().getRetiredClocks().get("B"));
        assertFalse(service.getParticipant("A").getDelivered().entries().containsKey("B"));
        assertFalse(service.getParticipant("C").getDelivered().entries().containsKey("B"));

        Message stale = new Message("B", 1, java.util.Map.of("B", 1L), "b1", 0L);
        assertEquals(ReceiveStatus.DUPLICATE, service.receive("A", stale).getStatus());

        service.join("D");
        Message a1 = service.broadcast("A", "a1");
        assertFalse(a1.getClock().containsKey("B"));
        assertEquals(List.of("a1"), payloads(service, "D"));
    }

    @Test
    void gcDoesNotRetireWhileActiveParticipantLags() {
        CausalBroadcastService service = newService(false);
        service.join("A");
        service.join("B");
        service.join("C");
        Message b1 = service.broadcast("B", "b1");
        service.receive("A", b1);
        service.leave("B");
        service.gc();

        assertFalse(service.getState().getRetiredClocks().containsKey("B"));
        assertEquals(ReceiveStatus.DELIVERED, service.receive("C", b1).getStatus());
        service.gc();
        assertEquals(1L, service.getState().getRetiredClocks().get("B"));
    }

    @Test
    void pullAdvancesCursorAndTrimsDeliveredLog() {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");
        for (int i = 1; i <= 5; i++) {
            service.broadcast("A", "m" + i);
        }

        PullResult page1 = service.pull("B", 2);
        assertEquals(List.of("m1", "m2"),
                page1.getMessages().stream().map(m -> m.getMessage().getPayload()).toList());
        assertEquals(2, page1.getCursor());

        PullResult page2 = service.pull("B", 2);
        assertEquals(List.of("m3", "m4"),
                page2.getMessages().stream().map(m -> m.getMessage().getPayload()).toList());

        PullResult page3 = service.pull("B", 2);
        assertEquals(List.of("m5"),
                page3.getMessages().stream().map(m -> m.getMessage().getPayload()).toList());
        assertTrue(service.pull("B", 2).getMessages().isEmpty());
        assertTrue(service.getParticipant("B").getDeliveredLog().isEmpty());
    }

    @Test
    void concurrentPullsAreDisjointAndComplete() throws InterruptedException {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");
        int total = 200;
        for (int i = 0; i < total; i++) {
            service.broadcast("A", "m" + i);
        }

        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            Thread thread = new Thread(() -> {
                while (true) {
                    PullResult result = service.pull("B", 7);
                    if (result.getMessages().isEmpty()) {
                        return;
                    }
                    result.getMessages().forEach(m -> seen.add(m.getMessage().getPayload()));
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(total, seen.size());
        Set<String> unique = Set.copyOf(seen);
        assertEquals(total, unique.size());
        Set<String> expected = java.util.stream.IntStream.range(0, total)
                .mapToObj(i -> "m" + i).collect(Collectors.toSet());
        assertEquals(expected, unique);
    }

    @Test
    void metadataDoesNotGrowWithMessageCount() {
        CausalBroadcastService service = newService(true);
        service.join("A");
        service.join("B");
        for (int i = 0; i < 500; i++) {
            service.broadcast("A", "m" + i);
            service.pull("B", 1000);
        }
        assertTrue(service.getParticipant("B").getDeliveredLog().isEmpty());
        assertEquals(1, service.getParticipant("B").getDelivered().entries().size());
        assertEquals(500, service.getParticipant("B").getDelivered().get("A"));
        assertEquals(0, service.getParticipant("B").getBuffer().size());
    }
}
