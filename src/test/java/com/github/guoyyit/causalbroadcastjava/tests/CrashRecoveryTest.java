package com.github.guoyyit.causalbroadcastjava.tests;

import com.github.guoyyit.causalbroadcastjava.model.BroadcastMessage;
import com.github.guoyyit.causalbroadcastjava.Application;
import com.github.guoyyit.causalbroadcastjava.service.CausalEngine;
import com.github.guoyyit.causalbroadcastjava.service.PullResult;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restart recovery: the service is closed (simulating a crash) and rebooted
 * against the same on-disk state.
 */
class CrashRecoveryTest {

    @Test
    void resumesAfterRestartWithoutDuplicateOrLoss(@TempDir Path dataDir) {
        ConfigurableApplicationContext crashed = boot(dataDir);
        CausalEngine before = crashed.getBean(CausalEngine.class);
        before.reset();
        before.join("alice");
        before.join("bob");

        BroadcastMessage first = before.broadcast("alice", "m1");
        BroadcastMessage second = before.broadcast("alice", "m2");

        // m2 arrives but stays buffered behind m1 at the moment of the crash.
        before.receive("bob", second.id());
        assertThat(payloads(before.pull("bob", 0L, 100))).isEmpty();
        crashed.close();

        // Reboot against the same data directory: buffer and clocks are restored.
        ConfigurableApplicationContext rebooted = boot(dataDir);
        CausalEngine after = rebooted.getBean(CausalEngine.class);
        assertThat(payloads(after.pull("bob", 0L, 100))).isEmpty();

        after.receive("bob", first.id());
        PullResult page = after.pull("bob", 0L, 100);
        assertThat(payloads(page)).containsExactly("m1", "m2");

        // Re-delivering after restart does not duplicate the delivered log.
        after.receive("bob", first.id());
        after.receive("bob", second.id());
        assertThat(after.pull("bob", 0L, 100).messages()).hasSize(2);

        // Work continues normally after recovery.
        BroadcastMessage third = after.broadcast("alice", "m3");
        after.receive("bob", third.id());
        assertThat(payloads(after.pull("bob", page.nextSeq(), 100)))
                .containsExactly("m3");
        rebooted.close();
    }

    private static ConfigurableApplicationContext boot(Path dataDir) {
        return new SpringApplicationBuilder(Application.class)
                .properties(
                        "causal.data-dir=" + dataDir,
                        "spring.datasource.url=jdbc:h2:file:" + dataDir
                                + "/causal-db;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .run();
    }

    private static List<String> payloads(PullResult result) {
        return result.messages().stream().map(BroadcastMessage::payload).toList();
    }
}
