package com.github.guoyyit.causalbroadcastjava.core;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A broadcast message envelope carrying its causal context (vector clock).
 * The message id is {@code senderId:seq} and is globally unique.
 */
public class Message {

    private String id;
    private String senderId;
    private long seq;
    private Map<String, Long> clock;
    private String payload;
    private long timestamp;

    public Message() {
    }

    public Message(String senderId, long seq, Map<String, Long> clock, String payload, long timestamp) {
        this.id = senderId + ":" + seq;
        this.senderId = senderId;
        this.seq = seq;
        this.clock = new TreeMap<>(clock);
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public Map<String, Long> getClock() {
        return clock;
    }

    public void setClock(Map<String, Long> clock) {
        this.clock = clock;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Message message)) {
            return false;
        }
        return Objects.equals(id, message.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Message{id=" + id + ", clock=" + clock + "}";
    }
}
