package com.github.guoyyit.causalbroadcastjava.web;

import com.github.guoyyit.causalbroadcastjava.model.BroadcastMessage;
import com.github.guoyyit.causalbroadcastjava.model.MemberKey;
import com.github.guoyyit.causalbroadcastjava.service.CausalEngine;
import com.github.guoyyit.causalbroadcastjava.service.PullResult;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * HTTP surface of the causal broadcast service. Skeleton.
 */
@RestController
public class CausalController {

    private final CausalEngine engine;

    public CausalController(CausalEngine engine) {
        this.engine = engine;
    }

    /** Registers a participant and returns its qualified (generation) key. */
    @PostMapping("/members/{memberId}")
    @ResponseStatus(HttpStatus.CREATED)
    public JoinResponse join(@PathVariable String memberId) {
        MemberKey memberKey = engine.join(memberId);
        return new JoinResponse(memberKey.id(), memberKey.gen(), memberKey.key());
    }

    /** Removes a participant after flushing in-flight messages. */
    @DeleteMapping("/members/{memberId}")
    public void leave(@PathVariable String memberId) {
        engine.leave(memberId);
    }

    /** Broadcasts a payload; the message (with id and causal clock) is returned. */
    @PostMapping("/broadcast/{senderId}")
    @ResponseStatus(HttpStatus.CREATED)
    public BroadcastResponse broadcast(
            @PathVariable String senderId,
            @RequestBody BroadcastRequest request) {
        BroadcastMessage message = engine.broadcast(senderId, request.payload());
        return new BroadcastResponse(message.id(), message.senderId(),
                message.seq(), message.clockJson(), message.payload());
    }

    /** Delivers a previously broadcast message to a receiver when causal order allows. */
    @PostMapping("/receive/{receiverId}/{messageId}")
    public void receive(
            @PathVariable String receiverId,
            @PathVariable String messageId) {
        engine.receive(receiverId, messageId);
    }

    /** Pulls delivered messages after a cursor; the returned nextSeq is the new cursor. */
    @GetMapping("/pull/{memberId}")
    public PullResponse pull(
            @PathVariable String memberId,
            @RequestParam(required = false) Long afterSeq,
            @RequestParam(defaultValue = "100") int limit) {
        PullResult result = engine.pull(memberId, afterSeq, limit);
        List<MessageView> views = result.messages().stream()
                .map(message -> new MessageView(message.id(), message.senderId(),
                        message.seq(), message.clockJson(), message.payload()))
                .toList();
        return new PullResponse(views, result.nextSeq());
    }

    /** Test/admin only: wipes all local state. */
    @PostMapping("/admin/reset")
    public void reset() {
        engine.reset();
    }

    public record JoinResponse(String memberId, int generation, String memberKey) {
    }

    public record BroadcastRequest(String payload) {
    }

    public record BroadcastResponse(
            String id, String senderId, long seq, String clockJson, String payload) {
    }

    public record MessageView(
            String id, String senderId, long seq, String clockJson, String payload) {
    }

    public record PullResponse(List<MessageView> messages, long nextSeq) {
    }
}
