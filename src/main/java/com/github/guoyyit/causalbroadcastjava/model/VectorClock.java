package com.github.guoyyit.causalbroadcastjava.model;

import java.util.Map;
import java.util.TreeMap;

/**
 * Vector clock over member component keys ({@link MemberKey#key()}).
 * Missing components read as zero.
 */
public final class VectorClock {

    private final Map<String, Long> entries;

    public VectorClock() {
        this(Map.of());
    }

    public VectorClock(Map<String, Long> entries) {
        TreeMap<String, Long> normalized = new TreeMap<>();
        entries.forEach((component, value) -> {
            if (component == null || value == null) {
                throw new IllegalArgumentException("clock entries must not contain nulls");
            }
            if (value > 0) {
                normalized.put(component, value);
            }
        });
        this.entries = Map.copyOf(normalized);
    }

    public Map<String, Long> entries() {
        return entries;
    }

    public long get(String component) {
        return entries.getOrDefault(component, 0L);
    }

    public VectorClock incremented(String component) {
        TreeMap<String, Long> next = new TreeMap<>(entries);
        next.merge(component, 1L, Long::sum);
        return new VectorClock(next);
    }

    public VectorClock mergedWith(Map<String, Long> other) {
        TreeMap<String, Long> next = new TreeMap<>(entries);
        other.forEach((component, value) -> {
            if (value > next.getOrDefault(component, 0L)) {
                next.put(component, value);
            }
        });
        return new VectorClock(next);
    }

    public VectorClock withoutComponent(String component) {
        TreeMap<String, Long> next = new TreeMap<>(entries);
        next.remove(component);
        return new VectorClock(next);
    }
}
