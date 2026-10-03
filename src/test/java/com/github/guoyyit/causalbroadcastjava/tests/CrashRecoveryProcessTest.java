package com.github.guoyyit.causalbroadcastjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end crash-recovery verification against real service processes:
 * the service is started as its own OS process, driven over HTTP, killed
 * with SIGKILL-equivalent force and restarted on the same data directory.
 * No in-process object reconstruction is involved.
 */
class CrashRecoveryProcessTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    @TempDir
    Path workDir;

    private record HttpResult(int status, String body) {
    }

    private static final class RunningService {
        private final Process process;
        private final int port;
        private final Path logFile;

        RunningService(Process process, int port, Path logFile) {
            this.process = process;
            this.port = port;
            this.logFile = logFile;
        }

        void killForcibly() throws InterruptedException {
            process.destroyForcibly();
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "process did not die");
        }
    }

    private RunningService start(Path dataDir, boolean autoFanout, String logName) throws Exception {
        Files.createDirectories(dataDir);
        int port = freePort();
        Path logFile = workDir.resolve(logName);
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder builder = new ProcessBuilder(
                javaBin,
                "-cp", System.getProperty("java.class.path"),
                "com.github.guoyyit.causalbroadcastjava.Application",
                "--server.port=" + port,
                "--causal.broadcast.data-dir=" + dataDir.toAbsolutePath(),
                "--causal.broadcast.auto-fanout=" + autoFanout,
                "--spring.main.banner-mode=off",
                "--logging.level.root=WARN");
        builder.redirectErrorStream(true);
        builder.redirectOutput(logFile.toFile());
        Process process = builder.start();
        awaitUp(process, port, logFile);
        return new RunningService(process, port, logFile);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void awaitUp(Process process, int port, Path logFile) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                fail("service process exited early with code " + process.exitValue()
                        + "; log:\n" + Files.readString(logFile));
            }
            try {
                HttpResult result = request("GET", port, "/api/participants", null);
                if (result.status() == 200) {
                    return;
                }
            } catch (IOException ignored) {
                // not up yet
            }
            Thread.sleep(250);
        }
        fail("service did not start within 90s; log:\n" + Files.readString(logFile));
    }

    private static HttpResult request(String method, int port, String path, String jsonBody)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10));
        if (jsonBody == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.method(method, HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .header("Content-Type", "application/json");
        }
        HttpResponse<String> response = HTTP.send(builder.build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return new HttpResult(response.statusCode(), response.body());
    }

    private static JsonNode json(HttpResult result) {
        try {
            return MAPPER.readTree(result.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void join(RunningService service, String id) throws Exception {
        assertEquals(200, request("POST", service.port, "/api/participants/" + id + "/join", null).status());
    }

    private static String broadcast(RunningService service, String sender, String payload) throws Exception {
        HttpResult result = request("POST", service.port, "/api/messages/broadcast",
                "{\"senderId\":\"" + sender + "\",\"payload\":\"" + payload + "\"}");
        assertEquals(200, result.status(), "broadcast failed: " + result.body());
        return result.body();
    }

    private static HttpResult receive(RunningService service, String receiver, String envelopeJson)
            throws Exception {
        return request("POST", service.port, "/api/messages/receive/" + receiver, envelopeJson);
    }

    private static JsonNode pull(RunningService service, String participant, int limit) throws Exception {
        HttpResult result = request("POST", service.port,
                "/api/participants/" + participant + "/pull?limit=" + limit, null);
        assertEquals(200, result.status());
        return json(result);
    }

    private static List<String> pullAllPayloads(RunningService service, String participant) throws Exception {
        List<String> payloads = new ArrayList<>();
        while (true) {
            JsonNode page = pull(service, participant, 100);
            for (JsonNode entry : page.get("messages")) {
                payloads.add(entry.get("message").get("payload").asText());
            }
            if (page.get("messages").isEmpty()) {
                return payloads;
            }
        }
    }

    private static JsonNode participant(RunningService service, String id) throws Exception {
        HttpResult result = request("GET", service.port, "/api/participants/" + id, null);
        assertEquals(200, result.status());
        return json(result);
    }

    @Test
    void restartAfterKillRecoversBufferCursorAndCausalMetadata() throws Exception {
        Path dataDir = workDir.resolve("exact-recovery");
        RunningService service = start(dataDir, false, "exact-recovery-1.log");
        try {
            join(service, "A");
            join(service, "B");
            String a1 = broadcast(service, "A", "a1");
            String a2 = broadcast(service, "A", "a2");
            String a3 = broadcast(service, "A", "a3");

            assertEquals("BUFFERED", json(receive(service, "B", a3)).get("status").asText());
            assertEquals("DELIVERED", json(receive(service, "B", a1)).get("status").asText());
            assertEquals("DUPLICATE", json(receive(service, "B", a1)).get("status").asText());

            JsonNode firstPage = pull(service, "B", 1);
            assertEquals(1, firstPage.get("cursor").asLong());
            assertEquals("a1", firstPage.get("messages").get(0).get("message").get("payload").asText());

            service.killForcibly();

            RunningService recovered = start(dataDir, false, "exact-recovery-2.log");
            try {
                JsonNode b = participant(recovered, "B");
                assertEquals(1, b.get("readCursor").asLong(), "read cursor must survive the crash");
                assertEquals(1, b.get("buffer").size(), "undelivered buffered message must survive");
                assertEquals(1, b.get("delivered").get("A").asLong(),
                        "causal delivery frontier must survive");

                assertEquals("DUPLICATE", json(receive(recovered, "B", a1)).get("status").asText(),
                        "already-delivered messages must not be delivered again after restart");

                JsonNode unblocked = json(receive(recovered, "B", a2));
                assertEquals("DELIVERED", unblocked.get("status").asText());
                assertEquals(2, unblocked.get("deliveredCount").asInt(),
                        "the buffered successor must drain in causal order");

                assertEquals(List.of("a2", "a3"), pullAllPayloads(recovered, "B"));
                JsonNode bAfter = participant(recovered, "B");
                assertEquals(3, bAfter.get("readCursor").asLong());
                assertEquals(3, bAfter.get("delivered").get("A").asLong());
                assertTrue(bAfter.get("buffer").isEmpty());

                JsonNode persisted = MAPPER.readTree(
                        Files.readString(dataDir.resolve("state.json")));
                assertEquals(3, persisted.get("participants").get("B").get("readCursor").asLong());
            } finally {
                recovered.killForcibly();
            }
        } finally {
            if (service.process.isAlive()) {
                service.killForcibly();
            }
        }
    }

    @Test
    void killDuringConcurrentLoadLeavesCompleteLoadableSnapshot() throws Exception {
        Path dataDir = workDir.resolve("concurrent-kill");
        RunningService service = start(dataDir, true, "concurrent-kill-1.log");
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger broadcastOk = new AtomicInteger();
        ConcurrentLinkedQueue<String> pulledBeforeKill = new ConcurrentLinkedQueue<>();
        try {
            join(service, "A");
            join(service, "B");

            Thread broadcaster = new Thread(() -> {
                int i = 0;
                while (!stop.get() && i < 500) {
                    i++;
                    try {
                        HttpResult result = request("POST", service.port, "/api/messages/broadcast",
                                "{\"senderId\":\"A\",\"payload\":\"m" + i + "\"}");
                        if (result.status() == 200) {
                            broadcastOk.incrementAndGet();
                        }
                    } catch (Exception e) {
                        if (stop.get()) {
                            return;
                        }
                    }
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            Thread pullerB = new Thread(() -> {
                while (!stop.get()) {
                    try {
                        JsonNode page = json(request("POST", service.port,
                                "/api/participants/B/pull?limit=20", null));
                        if (page.get("messages").isEmpty()) {
                            Thread.sleep(5);
                        } else {
                            for (JsonNode entry : page.get("messages")) {
                                pulledBeforeKill.add(entry.get("message").get("payload").asText());
                            }
                        }
                    } catch (Exception e) {
                        if (stop.get()) {
                            return;
                        }
                    }
                }
            });
            Thread pullerA = new Thread(() -> {
                while (!stop.get()) {
                    try {
                        request("POST", service.port, "/api/participants/A/pull?limit=50", null);
                        Thread.sleep(5);
                    } catch (Exception e) {
                        if (stop.get()) {
                            return;
                        }
                    }
                }
            });
            broadcaster.start();
            pullerB.start();
            pullerA.start();

            Thread.sleep(3000);
            service.killForcibly();
            stop.set(true);
            broadcaster.join(20000);
            pullerB.join(20000);
            pullerA.join(20000);

            RunningService recovered = start(dataDir, true, "concurrent-kill-2.log");
            try {
                JsonNode participants = json(request("GET", recovered.port, "/api/participants", null));
                assertEquals(2, participants.size(), "snapshot must contain both participants");

                JsonNode persisted = MAPPER.readTree(Files.readString(dataDir.resolve("state.json")));
                assertTrue(persisted.get("participants").has("A"));
                assertTrue(persisted.get("participants").has("B"));

                long sendSeq = participant(recovered, "A").get("sendSeq").asLong();
                long cursor = participant(recovered, "B").get("readCursor").asLong();
                assertTrue(sendSeq >= broadcastOk.get(),
                        "every acknowledged broadcast must be present in the snapshot");
                assertTrue(sendSeq >= 1, "load phase should have persisted broadcasts");

                List<String> before = new ArrayList<>(pulledBeforeKill);
                assertTrue(before.size() <= cursor,
                        "persisted cursor cannot lag behind acknowledged pulls");
                for (int i = 0; i < before.size(); i++) {
                    assertEquals("m" + (i + 1), before.get(i),
                            "pre-crash pulls must be a contiguous duplicate-free prefix");
                }

                List<String> after = pullAllPayloads(recovered, "B");
                List<String> expectedAfter = new ArrayList<>();
                for (long seq = cursor + 1; seq <= sendSeq; seq++) {
                    expectedAfter.add("m" + seq);
                }
                assertEquals(expectedAfter, after,
                        "post-restart delivery must resume exactly at the persisted cursor,"
                                + " with no gaps, duplicates or loss");

                String resumed = broadcast(recovered, "A", "after-restart");
                assertEquals(sendSeq + 1, MAPPER.readTree(resumed).get("seq").asLong());
                List<String> tail = pullAllPayloads(recovered, "B");
                assertEquals(List.of("after-restart"), tail);
            } finally {
                recovered.killForcibly();
            }
        } finally {
            stop.set(true);
            if (service.process.isAlive()) {
                service.killForcibly();
            }
        }
    }
}
