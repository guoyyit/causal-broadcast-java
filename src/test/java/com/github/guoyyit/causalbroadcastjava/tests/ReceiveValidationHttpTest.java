package com.github.guoyyit.causalbroadcastjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP-level contract of envelope validation: invalid envelopes are
 * rejected with 400 and a machine-readable reason, while 404/409
 * semantics for membership problems stay unchanged.
 */
@SpringBootTest
@AutoConfigureMockMvc
class ReceiveValidationHttpTest {

    private static final Path DATA_DIR = createTempDir();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("causal.broadcast.data-dir", () -> DATA_DIR.toString());
        registry.add("causal.broadcast.auto-fanout", () -> "true");
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("causal-broadcast-validation-http-test");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private void join(String id) throws Exception {
        mockMvc.perform(post("/api/participants/{id}/join", id))
                .andExpect(status().isOk());
    }

    private void expectRejection(String receiver, String envelopeJson, String reason) throws Exception {
        mockMvc.perform(post("/api/messages/receive/{receiverId}", receiver)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(envelopeJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value(reason))
                .andExpect(jsonPath("$.error").exists());
    }

    private List<String> pullAll(String participant) throws Exception {
        List<String> payloads = new ArrayList<>();
        long cursor = -1;
        while (true) {
            MvcResult result = mockMvc.perform(
                            post("/api/participants/{id}/pull", participant).param("limit", "100"))
                    .andExpect(status().isOk())
                    .andReturn();
            JsonNode page = objectMapper.readTree(result.getResponse().getContentAsString());
            for (JsonNode entry : page.get("messages")) {
                payloads.add(entry.get("message").get("payload").asText());
            }
            if (page.get("cursor").asLong() == cursor) {
                return payloads;
            }
            cursor = page.get("cursor").asLong();
        }
    }

    @Test
    void invalidEnvelopesReturn400WithDistinguishableReasons() throws Exception {
        join("http-sender");
        join("http-receiver");

        expectRejection("http-receiver",
                "{\"id\":\"ghost:1\",\"senderId\":\"ghost\",\"seq\":1,"
                        + "\"clock\":{\"ghost\":1},\"payload\":\"x\"}",
                "UNKNOWN_SENDER");
        expectRejection("http-receiver",
                "{\"id\":\"http-sender:0\",\"senderId\":\"http-sender\",\"seq\":0,"
                        + "\"clock\":{\"http-sender\":0},\"payload\":\"x\"}",
                "NON_POSITIVE_SEQUENCE");
        expectRejection("http-receiver",
                "{\"senderId\":\"http-sender\",\"seq\":1,"
                        + "\"clock\":{\"http-sender\":1},\"payload\":\"x\"}",
                "ENVELOPE_ID_MISMATCH");
        expectRejection("http-receiver",
                "{\"id\":\"http-sender:7\",\"senderId\":\"http-sender\",\"seq\":1,"
                        + "\"clock\":{\"http-sender\":1},\"payload\":\"x\"}",
                "ENVELOPE_ID_MISMATCH");
        expectRejection("http-receiver",
                "{\"id\":\"http-sender:1\",\"senderId\":\"http-sender\",\"seq\":1,"
                        + "\"payload\":\"x\"}",
                "MISSING_CAUSAL_METADATA");
        expectRejection("http-receiver",
                "{\"id\":\"http-sender:1\",\"senderId\":\"http-sender\",\"seq\":1,"
                        + "\"clock\":{\"http-sender\":5},\"payload\":\"x\"}",
                "CLOCK_SEQ_MISMATCH");
        expectRejection("http-receiver",
                "{\"id\":\"http-sender:1\",\"senderId\":\"http-sender\",\"seq\":1,"
                        + "\"clock\":{\"http-sender\":1,\"ghost\":2},\"payload\":\"x\"}",
                "UNKNOWN_CLOCK_REFERENCE");
    }

    @Test
    void membershipErrorSemanticsAreUnchanged() throws Exception {
        join("http-left");
        mockMvc.perform(delete("/api/participants/{id}", "http-left"))
                .andExpect(status().isOk());

        String legit = "{\"id\":\"http-left:1\",\"senderId\":\"http-left\",\"seq\":1,"
                + "\"clock\":{\"http-left\":1},\"payload\":\"x\"}";
        mockMvc.perform(post("/api/messages/receive/{receiverId}", "http-left")
                        .contentType(MediaType.APPLICATION_JSON).content(legit))
                .andExpect(status().isConflict());
        mockMvc.perform(post("/api/messages/receive/{receiverId}", "http-ghost")
                        .contentType(MediaType.APPLICATION_JSON).content(legit))
                .andExpect(status().isNotFound());
    }

    @Test
    void forgedEnvelopeThenGenuineJoinDeliversEverythingConsistently() throws Exception {
        join("e2e-a");
        join("e2e-b");

        String forged = "{\"id\":\"e2e-forger:1\",\"senderId\":\"e2e-forger\",\"seq\":1,"
                + "\"clock\":{\"e2e-forger\":1},\"payload\":\"forged\"}";
        expectRejection("e2e-a", forged, "UNKNOWN_SENDER");
        expectRejection("e2e-b", forged, "UNKNOWN_SENDER");

        join("e2e-forger");
        for (int i = 1; i <= 4; i++) {
            mockMvc.perform(post("/api/messages/broadcast")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"senderId\":\"e2e-forger\",\"payload\":\"m" + i + "\"}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.seq").value(i));
        }

        List<String> expected = List.of("m1", "m2", "m3", "m4");
        List<String> deliveredA = pullAll("e2e-a");
        List<String> deliveredB = pullAll("e2e-b");
        List<String> deliveredForger = pullAll("e2e-forger");
        assertEquals(expected, deliveredA);
        assertEquals(expected, deliveredB);
        assertEquals(expected, deliveredForger);
        assertEquals(deliveredA, deliveredB, "all receivers must converge to the same message set");
    }
}
