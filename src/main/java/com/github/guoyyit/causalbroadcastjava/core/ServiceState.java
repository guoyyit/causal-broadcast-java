package com.github.guoyyit.causalbroadcastjava.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Root persistent state of the broadcast service.
 */
public class ServiceState {

    private Map<String, ParticipantState> participants = new LinkedHashMap<>();
    private Map<String, Long> retiredClocks = new LinkedHashMap<>();

    public ServiceState() {
    }

    public Map<String, ParticipantState> getParticipants() {
        return participants;
    }

    public void setParticipants(Map<String, ParticipantState> participants) {
        this.participants = participants;
    }

    public Map<String, Long> getRetiredClocks() {
        return retiredClocks;
    }

    public void setRetiredClocks(Map<String, Long> retiredClocks) {
        this.retiredClocks = retiredClocks;
    }
}
