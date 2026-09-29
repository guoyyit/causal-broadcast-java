package com.github.guoyyit.causalbroadcastjava.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;
import java.util.TreeMap;

/**
 * Vector clock used to track the causal delivery frontier of a participant.
 * Serialized as a plain JSON object mapping participant id to counter.
 */
public class VectorClock {

    private final TreeMap<String, Long> entries = new TreeMap<>();

    @JsonCreator
    public VectorClock(Map<String, Long> entries) {
        if (entries != null) {
            entries.forEach(this::set);
        }
    }

    public VectorClock() {
    }

    public long get(String participantId) {
        return entries.getOrDefault(participantId, 0L);
    }

    public void set(String participantId, long value) {
        if (value <= 0L) {
            entries.remove(participantId);
        } else {
            entries.put(participantId, value);
        }
    }

    public void remove(String participantId) {
        entries.remove(participantId);
    }

    @JsonValue
    public Map<String, Long> entries() {
        return new TreeMap<>(entries);
    }

    public VectorClock copy() {
        return new VectorClock(entries);
    }

    @Override
    public String toString() {
        return entries.toString();
    }
}
