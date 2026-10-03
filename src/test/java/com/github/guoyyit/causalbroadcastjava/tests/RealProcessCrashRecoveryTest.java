package com.github.guoyyit.causalbroadcastjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end crash recovery against a real service process (not in-process
 * object reconstruction). A real JVM runs the Spring Boot service; members
 * join, broadcast, out-of-order/duplicate delivery and pulls happen over HTTP,
 * then the process is force-killed and restarted with the same data directory.
 */
class RealProcessCrashRecoveryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    Path dataDir;

    private final List<Process> processes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Process process : processes) {
            process.destroyForcibly();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * A real service process launched with {@code java -cp <test classpath>}
     * running the actual Spring Boot application.
     */
    private RealServiceProcess startService(Path dir, int port, boolean autoFanout) throws IOException {
        Path stdout = dir.resolve("stdout-" + port + ".log");
        Path stderr = dir.resolve("stderr-" + port + ".log");
        String javaHome = System.getProperty("java.home");
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder builder = new ProcessBuilder(
                javaHome + "/bin/java",
                "-cp", classpath,
                "com.github.guoyyit.causalbroadcastjava.Application",
                "--server.port=" + port,
                "--causal.broadcast.data-dir=" + dir.toAbsolutePath(),
                "--causal.broadcast.auto-fanout=" + autoFanout);
        builder.redirectOutput(stdout.toFile());
        builder.redirectError(stderr.toFile());
        Process process = builder.start();
        processes.add(process);
        return new RealServiceProcess(process, port, stdout, stderr);
    }

    private static final class RealServiceProcess {
        final Process process;
        final int port;
        final Path stdout;
        final Path stderr;

        RealServiceProcess(Process process, int port, Path stdout, Path stderr) {
            this.process = process;
            this.port = port;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        String baseUrl() {
            return "http://localhost:" + port;
        }

        void waitForReady() throws Exception {
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create(baseUrl() + "/api/participants"))
                    .GET().build();
            long deadline = System.currentTimeMillis() + 90_000;
            while (System.currentTimeMillis() < deadline) {
                if (!process.isAlive()) {
                    throw new IllegalStateException(
                            "service process exited early; see " + stderr);
                }
                try {
                    HttpResponse<String> response =
                            HTTP.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        return;
                    }
                } catch (Exception ignored) {
                    // not ready yet
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("service did not become ready; see " + stderr);
        }

        void kill() throws InterruptedException {
            process.destroyForcibly();
            process.waitFor(15, TimeUnit.SECONDS);
        }
    }

    private static HttpResponse<String> post(String base, String path, String body)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode postJson(String base, String path, String body) throws Exception {
        HttpResponse<String> response = post(base, path, body);
        assertEquals(200, response.statusCode(),
                "POST " + path + " failed: " + response.body());
        return MAPPER.readTree(response.body());
    }

    private static JsonNode pull(String base, String receiver, int limit) throws Exception {
        return postJson(base, "/api/participants/" + receiver + "/pull?limit=" + limit, "");
    }

    private static JsonNode getJson(String base, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + path))
                .GET().build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(),
                "GET " + path + " failed: " + response.body());
        return MAPPER.readTree(response.body());
    }

    private static List<String> payloads(JsonNode pullResult) {
        List<String> result = new ArrayList<>();
        for (JsonNode delivered : pullResult.get("messages")) {
            result.add(delivered.get("message").get("payload").asText());
        }
        return result;
    }

    @Test
    void crashAfterPartialDeliveryAndPullResumesFromSnapshot() throws Exception {
        int port1 = freePort();
        RealServiceProcess first = startService(dataDir, port1, false);
        first.waitForReady();
        String base = first.baseUrl();

        postJson(base, "/api/participants/A/join", "");
        postJson(base, "/api/participants/B/join", "");
        postJson(base, "/api/participants/C/join", "");

        String a1 = postJson(base, "/api/messages/broadcast",
                "{\"senderId\":\"A\",\"payload\":\"a1\"}").toString();
        String a2 = postJson(base, "/api/messages/broadcast",
                "{\"senderId\":\"A\",\"payload\":\"a2\"}").toString();
        String a3 = postJson(base, "/api/messages/broadcast",
                "{\"senderId\":\"A\",\"payload\":\"a3\"}").toString();
        String b1 = postJson(base, "/api/messages/broadcast",
                "{\"senderId\":\"B\",\"payload\":\"b1\"}").toString();

        // Out-of-order arrival at C: a3 buffered, b1 delivered (concurrent),
        // a1 delivered but a2 still missing so a3 stays buffered.
        assertEquals("BUFFERED", postJson(base, "/api/messages/receive/C", a3)
                .get("status").asText());
        assertEquals("DELIVERED", postJson(base, "/api/messages/receive/C", b1)
                .get("status").asText());
        assertEquals("DELIVERED", postJson(base, "/api/messages/receive/C", a1)
                .get("status").asText());

        // Partial pull advances the read cursor.
        JsonNode firstPage = pull(base, "C", 2);
        assertEquals(List.of("b1", "a1"), payloads(firstPage));
        assertEquals(2, firstPage.get("cursor").asLong());

        // Duplicate arrival is deduplicated.
        assertEquals("DUPLICATE", postJson(base, "/api/messages/receive/C", a1)
                .get("status").asText());

        // Force-kill the real process, then restart with the same data dir.
        first.kill();
        assertFalse(first.process.isAlive());

        int port2 = freePort();
        RealServiceProcess restarted = startService(dataDir, port2, false);
        restarted.waitForReady();
        String base2 = restarted.baseUrl();

        // Read cursor recovered: pull continues after cursor 2, no re-delivery.
        JsonNode afterRestart = pull(base2, "C", 100);
        assertTrue(afterRestart.get("messages").isEmpty());

        // The buffered a3 continues in causal order once its missing
        // predecessor a2 is injected after recovery: a2 delivers and drains
        // a3 in the same receive call.
        JsonNode drained = postJson(base2, "/api/messages/receive/C", a2);
        assertEquals("DELIVERED", drained.get("status").asText());
        assertEquals(2, drained.get("deliveredCount").asInt());

        JsonNode stateC = getJson(base2, "/api/participants/C");
        assertEquals(2, stateC.get("readCursor").asLong());
        assertEquals(2, stateC.get("deliveredLog").size());
        assertEquals("a2", stateC.get("deliveredLog").get(0)
                .get("message").get("payload").asText());
        assertEquals("a3", stateC.get("deliveredLog").get(1)
                .get("message").get("payload").asText());
        assertTrue(stateC.get("buffer").isEmpty());

        // Pull returns the newly delivered messages and advances the cursor.
        JsonNode page = pull(base2, "C", 100);
        assertEquals(List.of("a2", "a3"), payloads(page));
        assertEquals(4, page.get("cursor").asLong());

        // Causal metadata recovered: delivery frontier covers A:3 and B:1.
        JsonNode delivered = stateC.get("delivered");
        assertEquals(3, delivered.get("A").asLong());
        assertEquals(1, delivered.get("B").asLong());

        // Dedup survives the crash.
        assertEquals("DUPLICATE", postJson(base2, "/api/messages/receive/C", a1)
                .get("status").asText());
        assertEquals("DUPLICATE", postJson(base2, "/api/messages/receive/C", a2)
                .get("status").asText());

        // A new participant can join and pull the full causal history.
        postJson(base2, "/api/participants/D/join", "");
        JsonNode dPage = pull(base2, "D", 100);
        assertTrue(dPage.get("messages").isEmpty());
    }

    @Test
    void crashDuringConcurrentLoadLeavesLoadableSnapshot() throws Exception {
        int port = freePort();
        RealServiceProcess service = startService(dataDir, port, true);
        service.waitForReady();
        String base = service.baseUrl();

        postJson(base, "/api/participants/A/join", "");
        postJson(base, "/api/participants/B/join", "");
        postJson(base, "/api/participants/C/join", "");

        ExecutorService pool = Executors.newFixedThreadPool(6);
        try {
            // Concurrent broadcasters, receivers (pull) and a joiner.
            for (int i = 0; i < 2; i++) {
                String sender = i == 0 ? "A" : "B";
                String prefix = sender.toLowerCase();
                pool.submit(() -> {
                    int n = 0;
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            post(base, "/api/messages/broadcast",
                                    "{\"senderId\":\"" + sender + "\",\"payload\":\"" + prefix + "-" + n + "\"}");
                            n++;
                        } catch (Exception ignored) {
                            // service may be briefly unavailable under load
                        }
                    }
                });
            }
            for (int i = 0; i < 2; i++) {
                String receiver = i == 0 ? "C" : "A";
                pool.submit(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            post(base, "/api/participants/" + receiver + "/pull?limit=50", "");
                        } catch (Exception ignored) {
                            // service may be briefly unavailable under load
                        }
                    }
                });
            }
            pool.submit(() -> {
                try {
                    Thread.sleep(1500);
                    post(base, "/api/participants/D/join", "");
                } catch (Exception ignored) {
                    // ignore
                }
            });

            Thread.sleep(5000);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(15, TimeUnit.SECONDS);
        }

        // Force-kill the process mid-load.
        service.kill();
        assertFalse(service.process.isAlive());

        // The snapshot must be a complete, loadable JSON document.
        Path stateFile = dataDir.resolve("state.json");
        assertTrue(Files.exists(stateFile), "state.json must exist after crash");
        JsonNode snapshot = MAPPER.readTree(stateFile.toFile());
        assertTrue(snapshot.has("participants"));
        assertTrue(snapshot.get("participants").size() >= 3);

        // Restart with the same data dir and verify the state loads.
        int port2 = freePort();
        RealServiceProcess restarted = startService(dataDir, port2, true);
        restarted.waitForReady();
        String base2 = restarted.baseUrl();

        JsonNode participants = getJson(base2, "/api/participants");
        assertTrue(participants.size() >= 3);

        // Causal invariant: no duplicate delivered messages and every
        // delivered message's causal predecessors precede it.
        for (JsonNode participant : participants) {
            assertCausalInvariant(participant);
        }

        // The service keeps serving after recovery.
        postJson(base2, "/api/messages/broadcast",
                "{\"senderId\":\"A\",\"payload\":\"after-restart\"}");
        JsonNode page = pull(base2, "A", 1000);
        boolean found = false;
        for (String payload : payloads(page)) {
            if (payload.equals("after-restart")) {
                found = true;
            }
        }
        assertTrue(found, "service must keep serving after recovery");
    }

    /**
+     * Verifies the causal delivery invariant for one participant: no message
     * is delivered twice, and for every delivered message all causal
     * predecessors (clock entries) have already been delivered.
     */
    private void assertCausalInvariant(JsonNode participant) {
        List<JsonNode> log = new ArrayList<>();
        participant.get("deliveredLog").forEach(log::add);
        // The delivery frontier is the max seq ever delivered per sender.
        // Late joiners have a non-zero frontier with an empty log, so the
        // frontier (not the log count) is the correct baseline.
        Map<String, Long> frontier = new HashMap<>();
        participant.get("delivered").fields()
                .forEachRemaining(e -> frontier.put(e.getKey(), e.getValue().asLong()));
        Set<String> seenIds = new HashSet<>();
        for (JsonNode delivered : log) {
            JsonNode message = delivered.get("message");
            String id = message.get("id").asText();
            assertTrue(seenIds.add(id), "duplicate delivered message: " + id);
            String sender = message.get("senderId").asText();
            long senderSeq = message.get("seq").asLong();
            assertTrue(frontier.getOrDefault(sender, 0L) >= senderSeq,
                    "state inconsistency: " + id + " seq " + senderSeq
                            + " exceeds frontier " + frontier.getOrDefault(sender, 0L));
            Iterator<Map.Entry<String, JsonNode>> clock = message.get("clock").fields();
            while (clock.hasNext()) {
                Map.Entry<String, JsonNode> entry = clock.next();
                String predecessor = entry.getKey();
                long required = entry.getValue().asLong();
                long current = frontier.getOrDefault(predecessor, 0L);
                assertTrue(current >= required,
                        "causal violation: " + id + " requires " + predecessor + ":" + required
                                + " but frontier is " + current);
            }
        }
    }
}
