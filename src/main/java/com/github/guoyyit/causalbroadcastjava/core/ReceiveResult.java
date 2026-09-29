package com.github.guoyyit.causalbroadcastjava.core;

/**
 * Result of a receive call, including any messages that became deliverable
 * as a consequence (the message itself plus drained buffer entries).
 */
public class ReceiveResult {

    private ReceiveStatus status;
    private int deliveredCount;

    public ReceiveResult() {
    }

    public ReceiveResult(ReceiveStatus status, int deliveredCount) {
        this.status = status;
        this.deliveredCount = deliveredCount;
    }

    public ReceiveStatus getStatus() {
        return status;
    }

    public void setStatus(ReceiveStatus status) {
        this.status = status;
    }

    public int getDeliveredCount() {
        return deliveredCount;
    }

    public void setDeliveredCount(int deliveredCount) {
        this.deliveredCount = deliveredCount;
    }
}
