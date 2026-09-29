package com.github.guoyyit.causalbroadcastjava.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Persistent per-participant state: membership, send/delivery clocks,
 * undeliverable buffer, delivered log and read cursor.
 */
public class ParticipantState {

    private String id;
    private boolean active;
    private long sendSeq;
    private VectorClock delivered = new VectorClock();
    private List<Message> buffer = new ArrayList<>();
    private List<DeliveredMessage> deliveredLog = new ArrayList<>();
    private long deliverySeq;
    private long readCursor;

    public ParticipantState() {
    }

    public ParticipantState(String id, boolean active) {
        this.id = id;
        this.active = active;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public long getSendSeq() {
        return sendSeq;
    }

    public void setSendSeq(long sendSeq) {
        this.sendSeq = sendSeq;
    }

    public VectorClock getDelivered() {
        return delivered;
    }

    public void setDelivered(VectorClock delivered) {
        this.delivered = delivered;
    }

    public List<Message> getBuffer() {
        return buffer;
    }

    public void setBuffer(List<Message> buffer) {
        this.buffer = buffer;
    }

    public List<DeliveredMessage> getDeliveredLog() {
        return deliveredLog;
    }

    public void setDeliveredLog(List<DeliveredMessage> deliveredLog) {
        this.deliveredLog = deliveredLog;
    }

    public long getDeliverySeq() {
        return deliverySeq;
    }

    public void setDeliverySeq(long deliverySeq) {
        this.deliverySeq = deliverySeq;
    }

    public long getReadCursor() {
        return readCursor;
    }

    public void setReadCursor(long readCursor) {
        this.readCursor = readCursor;
    }

    /**
     * Deep-copies this state so callers cannot mutate internal persistent state.
     */
    public ParticipantState copy() {
        ParticipantState copy = new ParticipantState(id, active);
        copy.setSendSeq(sendSeq);
        copy.setDelivered(delivered.copy());
        copy.setBuffer(new ArrayList<>(buffer));
        copy.setDeliveredLog(new ArrayList<>(deliveredLog));
        copy.setDeliverySeq(deliverySeq);
        copy.setReadCursor(readCursor);
        return copy;
    }
}
