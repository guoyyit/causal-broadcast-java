package com.github.guoyyit.causalbroadcastjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.guoyyit.causalbroadcastjava.service.CausalEngine;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CausalHttpTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private CausalEngine engine;

    @Test
    void fullFlowOverHttp() {
        engine.reset();

        ResponseEntity<String> alice = post("/members/alice", null);
        assertThat(alice.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(alice.getBody()).contains("\"generation\":1");

        assertThat(post("/members/bob", null).getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> broadcast = post("/broadcast/alice", "{\"payload\":\"hello\"}");
        assertThat(broadcast.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String messageId = Json.read(broadcast.getBody()).get("id").asText();

        ResponseEntity<String> receive = post("/receive/bob/" + messageId, null);
        assertThat(receive.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Duplicate receive is accepted and remains idempotent.
        assertThat(post("/receive/bob/" + messageId, null).getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<String> pull = rest.getForEntity(
                url("/pull/bob?afterSeq=0&limit=10"), String.class);
        assertThat(pull.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode pullBody = Json.read(pull.getBody());
        assertThat(pullBody.get("messages")).hasSize(1);
        assertThat(pullBody.get("messages").get(0).get("payload").asText()).isEqualTo("hello");
        assertThat(pullBody.get("nextSeq").asLong()).isEqualTo(1);

        assertThat(rest.exchange(url("/members/bob"), HttpMethod.DELETE, null, String.class)
                .getStatusCode()).isEqualTo(HttpStatus.OK);

        // Errors map to meaningful status codes.
        assertThat(post("/broadcast/ghost", "{\"payload\":\"x\"}").getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(post("/receive/ghost/does-not-exist", null).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(post("/receive/alice/unknown-message-id", null).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    private ResponseEntity<String> post(String path, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(jsonBody, headers);
        return rest.postForEntity(url(path), entity, String.class);
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static final class Json {
        private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
                new com.fasterxml.jackson.databind.ObjectMapper();

        static JsonNode read(String body) {
            try {
                return MAPPER.readTree(body);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
