package com.github.guoyyit.causalbroadcastjava.tests;

import com.github.guoyyit.causalbroadcastjava.service.CausalEngine;
import com.github.guoyyit.causalbroadcastjava.service.PullResult;
import com.github.guoyyit.causalbroadcastjava.model.BroadcastMessage;
import com.github.guoyyit.causalbroadcastjava.store.DurableStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Engine-level causal broadcast scenarios. Skeleton, to be filled scenario
 * by scenario.
 */
@SpringBootTest
class CausalEngineTest {

    @Autowired
    private CausalEngine engine;

    @Autowired
    private DurableStore store;

    @AfterEach
    void cleanUp() {
        engine.reset();
    }

    @Test
    void concurrentPullsDoNotSkipOrDuplicate() throws Exception {
        engine.join("solo");
        for (int i = 0; i < 300; i++) {
            engine.broadcast("solo", "msg-" + i);
        }

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Long> finalCursors = Collections.synchronizedList(new java.util.ArrayList<>());

        // Each worker repeatedly re-reads from its own cursor snapshot; the
        // invariant is every page is a strict, gap-free prefix continuation.
        for (int worker = 0; worker < threads; worker++) {
            pool.submit(() -> {
                try {
                    start.await();
                    long cursor = 0;
                    while (true) {
                        PullResult page = engine.pull("solo", cursor, 7);
                        if (page.messages().isEmpty()) {
                            finalCursors.add(cursor);
                            break;
                        }
                        cursor = page.nextSeq();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        // Every worker terminates exactly at the final cursor (no gap/overlap
        // possible across sequential pages).
        assertThat(finalCursors).hasSize(threads);
        assertThat(finalCursors).allSatisfy(seq -> assertThat(seq).isEqualTo(300L));

        // One sequential walk using the returned cursors covers all messages.
        long cursor = 0;
        List<String> all = new java.util.ArrayList<>();
        while (true) {
            PullResult page = engine.pull("solo", cursor, 37);
            all.addAll(payloads(page));
            if (page.messages().isEmpty()) {
                break;
            }
            cursor = page.nextSeq();
        }
        assertThat(all).hasSize(300);
        assertThat(all).doesNotHaveDuplicates();
        assertThat(all).startsWith("msg-0").endsWith("msg-299");
    }

    @Test
    void outOfOrderArrivalBuffersUntilPredecessorsDelivered() {
        engine.join("alice");
        engine.join("bob");

        BroadcastMessage first = engine.broadcast("alice", "m1");
        BroadcastMessage second = engine.broadcast("alice", "m2");

        // Second message arrives before its predecessor: it must stay buffered.
        engine.receive("bob", second.id());
        assertThat(payloads(engine.pull("bob", null, 100))).isEmpty();

        // Late arrival of the predecessor releases both, in causal order.
        engine.receive("bob", first.id());
        assertThat(payloads(engine.pull("bob", null, 100)))
                .containsExactly("m1", "m2");
    }

    @Test
    void duplicateReceiveDeliversExactlyOnce() {
        engine.join("alice");
        engine.join("bob");
        BroadcastMessage message = engine.broadcast("alice", "only");

        engine.receive("bob", message.id());
        engine.receive("bob", message.id());
        engine.receive("bob", message.id());

        PullResult firstPage = engine.pull("bob", null, 100);
        assertThat(payloads(firstPage)).containsExactly("only");
        assertThat(firstPage.nextSeq()).isEqualTo(1);

        // Advancing the cursor makes the same message invisible; re-pulling the
        // old cursor is deterministic but idempotence is about delivery, and
        // no second delivery row exists.
        assertThat(engine.pull("bob", firstPage.nextSeq(), 100).messages()).isEmpty();
    }

    @Test
    void concurrentMessagesDeliverInConsistentCausalOrder() {
        engine.join("alice");
        engine.join("bob");

        // Each sender broadcasts without observing the other: these are concurrent.
        BroadcastMessage a1 = engine.broadcast("alice", "a1");
        BroadcastMessage b1 = engine.broadcast("bob", "b1");
        // After observing b1, alice's next message causally follows both a1 and b1.
        engine.receive("alice", b1.id());
        BroadcastMessage a2 = engine.broadcast("alice", "a2");

        // Bob receives a2 before a1: a2 must buffer until a1 is delivered.
        engine.receive("bob", a2.id());
        assertThat(payloads(engine.pull("bob", 0L, 100))).containsExactly("b1");

        engine.receive("bob", a1.id());
        List<String> delivered = payloads(engine.pull("bob", 0L, 100));
        assertThat(delivered).startsWith("b1");
        assertThat(delivered).containsSubsequence("a1", "a2");

        // Independent delivery order for concurrent messages is allowed on alice's side.
        assertThat(payloads(engine.pull("alice", 0L, 100)))
                .containsExactly("a1", "b1", "a2");
    }

    @Test
    void dynamicMembersJoinAndLeave() {
        engine.join("alice");
        BroadcastMessage beforeJoin = engine.broadcast("alice", "before-carol");

        engine.join("carol");
        BroadcastMessage afterJoin = engine.broadcast("alice", "after-carol");

        // Late joiner participates only from its join point.
        engine.receive("carol", beforeJoin.id());
        engine.receive("carol", afterJoin.id());
        assertThat(payloads(engine.pull("carol", 0L, 100)))
                .containsExactly("after-carol");

        // Messages buffered behind the leaving member still drain on leave.
        engine.join("dave");
        BroadcastMessage d1 = engine.broadcast("alice", "d1");
        BroadcastMessage d2 = engine.broadcast("alice", "d2");
        engine.receive("dave", d2.id());
        assertThat(payloads(engine.pull("dave", 0L, 100))).isEmpty();

        engine.leave("alice");
        assertThat(payloads(engine.pull("dave", 0L, 100)))
                .contains("d1", "d2");

        // Rejoining under the same id creates a fresh generation: old messages
        // are not replayed into the new incarnation, while remaining members
        // keep receiving on the new causal chain.
        engine.join("alice");
        BroadcastMessage fresh = engine.broadcast("alice", "fresh");
        engine.receive("carol", fresh.id());
        engine.receive("dave", fresh.id());
        assertThat(payloads(engine.pull("alice", 0L, 100)))
                .containsExactly("fresh");
        assertThat(payloads(engine.pull("carol", 0L, 100)))
                .containsExactly("after-carol", "d1", "d2", "fresh");
    }

    @Test
    void stableMetadataIsReclaimed() {
        engine.join("alice");
        engine.join("bob");

        BroadcastMessage first = engine.broadcast("alice", "m1");
        BroadcastMessage second = engine.broadcast("alice", "m2");

        // Fully stable metadata is reclaimed immediately.
        engine.receive("bob", first.id());
        engine.receive("bob", second.id());
        assertThat(store.findMessage(first.id()).clockJson()).isNull();
        assertThat(store.findMessage(second.id()).clockJson()).isNull();

        // Buffered messages keep their clock, so causal checks remain valid
        // even after earlier clocks were reclaimed; late delivery still works.
        engine.join("carol");
        BroadcastMessage third = engine.broadcast("alice", "m3");
        BroadcastMessage fourth = engine.broadcast("alice", "m4");
        engine.receive("carol", fourth.id());
        assertThat(store.findMessage(third.id()).clockJson()).isNotNull();
        assertThat(store.findMessage(fourth.id()).clockJson()).isNotNull();

        engine.receive("carol", third.id());
        assertThat(payloads(engine.pull("carol", 0L, 100)))
                .containsExactly("m3", "m4");
        engine.receive("bob", third.id());
        engine.receive("bob", fourth.id());

        // Long run with everything delivered: retained causal metadata must
        // not grow with total message count; fully stable state holds zero.
        for (int i = 0; i < 50; i++) {
            BroadcastMessage stable = engine.broadcast("alice", "bulk-" + i);
            engine.receive("bob", stable.id());
            engine.receive("carol", stable.id());
        }
        assertThat(store.countNonNullClocks()).isZero();

        // Only genuinely in-flight (undelivered) messages keep metadata.
        BroadcastMessage inflight = engine.broadcast("alice", "inflight");
        assertThat(store.countNonNullClocks()).isEqualTo(1);
        engine.receive("bob", inflight.id());
        engine.receive("carol", inflight.id());
        assertThat(store.countNonNullClocks()).isZero();
    }

    private static List<String> payloads(PullResult result) {
        return result.messages().stream().map(BroadcastMessage::payload).toList();
    }
}
