package com.github.guoyyit.causalbroadcastjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.guoyyit.causalbroadcastjava.core.Message;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP contract for envelope validation: malformed/forged envelopes yield 400
 * with a distinguishable reason, unknown receivers yield 404, left receivers
 * yield 409, and a rejected forged envelope does not affect later delivery.
 */
@SpringBootTest
@AutoConfigureMockMvc
class EnvelopeValidationHttpTest {

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

    private String json(Message message) throws Exception {
        return objectMapper.writeValueAsString(message);
    }

    private Message envelope(String sender, long seq, String id, Map<String, Long> clock) {
        Message message = new Message(sender, seq, clock == null ? Map.of() : clock, "payload", 0L);
        message.setId(id);
        if (clock == null) {
            message.setClock(null);
        }
        return message;
    }

    @Test
    void forgedEnvelopeIsRejectedWith400AndLaterJoinDeliversAllMessages() throws Exception {
        mockMvc.perform(post("/api/participants/{id}/join", "a"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/participants/{id}/join", "b"))
                .andExpect(status().isOk());

        Message forged = envelope("ghost", 1, "ghost:1", Map.of("ghost", 1L));
        mockMvc.perform(post("/api/messages/receive/{receiverId}", "a")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(forged)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("UNKNOWN_SENDER"));

        mockMvc.perform(post("/api/participants/{id}/join", "ghost"))
                .andExpect(status().isOk());
        for (int i = 1; i <= 3; i++) {
            mockMvc.perform(post("/api/messages/broadcast")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"senderId\":\"ghost\",\"payload\":\"g" + i + "\"}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.seq").value(i));
        }

        for (String receiver : new String[]{"a", "b"}) {
            mockMvc.perform(post("/api/participants/{id}/pull", receiver).param("limit", "100"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.messages.length()").value(3))
                    .andExpect(jsonPath("$.messages[0].message.payload").value("g1"))
                    .andExpect(jsonPath("$.messages[1].message.payload").value("g2"))
                    .andExpect(jsonPath("$.messages[2].message.payload").value("g3"));
        }
    }

    @Test
    void structuralViolationsReturn400WithDistinguishableReasons() throws Exception {
        mockMvc.perform(post("/api/participants/{id}/join", "a"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/messages/broadcast")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"senderId\":\"a\",\"payload\":\"a1\"}"))
                .andExpect(status().isOk());

        expectBadRequest("a", envelope("a", 0, "a:0", Map.of("a", 0L)), "INVALID_SEQ");
        expectBadRequest("a", envelope("a", 1, "a:99", Map.of("a", 1L)), "INVALID_ENVELOPE_ID");
        expectBadRequest("a", envelope("a", 1, "a:1", null), "MISSING_CLOCK");
        expectBadRequest("a", envelope("a", 1, "a:1", Map.of("a", 2L)), "CLOCK_SEQ_MISMATCH");
        expectBadRequest("a", envelope("a", 2, "a:2", Map.of("a", 2L, "never-joined", 1L)),
                "UNKNOWN_CLOCK_ENTRY");
    }

    private void expectBadRequest(String receiver, Message envelope, String reason) throws Exception {
        mockMvc.perform(post("/api/messages/receive/{receiverId}", receiver)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(envelope)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value(reason));
    }

    @Test
    void receiveToUnknownReceiverReturns404() throws Exception {
        mockMvc.perform(post("/api/participants/{id}/join", "a"))
                .andExpect(status().isOk());
        Message valid = envelope("a", 1, "a:1", Map.of("a", 1L));
        mockMvc.perform(post("/api/messages/receive/{receiverId}", "nobody")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(valid)))
                .andExpect(status().isNotFound());
    }

    @Test
    void receiveToLeftReceiverReturns409() throws Exception {
        mockMvc.perform(post("/api/participants/{id}/join", "a"))
                .andExpect(status().isOk());
        mockMvc.perform(delete("/api/participants/{id}", "a"))
                .andExpect(status().isOk());
        Message valid = envelope("a", 1, "a:1", Map.of("a", 1L));
        mockMvc.perform(post("/api/messages/receive/{receiverId}", "a")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json(valid)))
                .andExpect(status().isConflict());
    }
}
