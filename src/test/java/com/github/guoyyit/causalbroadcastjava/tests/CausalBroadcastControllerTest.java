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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class CausalBroadcastControllerTest {

    private static final Path DATA_DIR = createTempDir();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("causal.broadcast.data-dir", () -> DATA_DIR.toString());
        registry.add("causal.broadcast.auto-fanout", () -> "false");
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("causal-broadcast-controller-test");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void endToEndBroadcastAndPullOverHttp() throws Exception {
        mockMvc.perform(post("/api/participants/{id}/join", "alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.active").value(true));
        mockMvc.perform(post("/api/participants/{id}/join", "bob"))
                .andExpect(status().isOk());

        MvcResult broadcastResult = mockMvc.perform(post("/api/messages/broadcast")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"senderId\":\"alice\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("alice:1"))
                .andExpect(jsonPath("$.seq").value(1))
                .andReturn();
        JsonNode message = objectMapper.readTree(broadcastResult.getResponse().getContentAsString());

        mockMvc.perform(post("/api/messages/receive/{receiverId}", "bob")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(message)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DELIVERED"));

        mockMvc.perform(post("/api/messages/receive/{receiverId}", "bob")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(message)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DUPLICATE"));

        mockMvc.perform(post("/api/participants/{id}/pull", "bob").param("limit", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cursor").value(1))
                .andExpect(jsonPath("$.messages[0].message.payload").value("hello"));

        mockMvc.perform(post("/api/participants/{id}/pull", "bob").param("limit", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messages").isEmpty());

        mockMvc.perform(get("/api/participants"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));

        mockMvc.perform(delete("/api/participants/{id}", "bob"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/messages/broadcast")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"senderId\":\"bob\",\"payload\":\"x\"}"))
                .andExpect(status().isConflict());
        mockMvc.perform(post("/api/participants/{id}/pull", "ghost"))
                .andExpect(status().isNotFound());
        mockMvc.perform(post("/api/admin/gc"))
                .andExpect(status().isOk());

        assertTrue(Files.exists(DATA_DIR.resolve("state.json")));
        JsonNode persisted = objectMapper.readTree(Files.readString(DATA_DIR.resolve("state.json")));
        assertEquals(1, persisted.get("participants").get("alice").get("sendSeq").asLong());
    }
}
