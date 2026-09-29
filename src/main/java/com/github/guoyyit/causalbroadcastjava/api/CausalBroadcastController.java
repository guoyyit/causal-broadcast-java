package com.github.guoyyit.causalbroadcastjava.api;

import com.github.guoyyit.causalbroadcastjava.core.Message;
import com.github.guoyyit.causalbroadcastjava.core.ParticipantState;
import com.github.guoyyit.causalbroadcastjava.core.PullResult;
import com.github.guoyyit.causalbroadcastjava.core.ReceiveResult;
import com.github.guoyyit.causalbroadcastjava.service.CausalBroadcastService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Local HTTP API of the causal broadcast service.
 */
@RestController
@RequestMapping("/api")
public class CausalBroadcastController {

    private final CausalBroadcastService service;

    public CausalBroadcastController(CausalBroadcastService service) {
        this.service = service;
    }

    @PostMapping("/participants/{id}/join")
    public ParticipantState join(@PathVariable String id) {
        return service.join(id);
    }

    @DeleteMapping("/participants/{id}")
    public Map<String, Object> leave(@PathVariable String id) {
        service.leave(id);
        return Map.of("participant", id, "active", false);
    }

    @GetMapping("/participants")
    public List<ParticipantState> listParticipants() {
        return service.listParticipants();
    }

    @GetMapping("/participants/{id}")
    public ParticipantState getParticipant(@PathVariable String id) {
        return service.getParticipant(id);
    }

    @PostMapping("/messages/broadcast")
    public Message broadcast(@RequestBody BroadcastRequest request) {
        return service.broadcast(request.getSenderId(), request.getPayload());
    }

    @PostMapping("/messages/receive/{receiverId}")
    public ReceiveResult receive(@PathVariable String receiverId, @RequestBody Message message) {
        return service.receive(receiverId, message);
    }

    @PostMapping("/participants/{id}/pull")
    public PullResult pull(@PathVariable String id, @RequestParam(defaultValue = "100") int limit) {
        return service.pull(id, limit);
    }

    @PostMapping("/admin/gc")
    public Map<String, Object> gc() {
        service.gc();
        return Map.of("status", "ok");
    }

    public static class BroadcastRequest {
        private String senderId;
        private String payload;

        public String getSenderId() {
            return senderId;
        }

        public void setSenderId(String senderId) {
            this.senderId = senderId;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }
    }
}
