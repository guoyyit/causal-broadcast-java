package com.github.guoyyit.causalbroadcastjava.core;

/**
 * A message that has been delivered to a participant, tagged with the
 * participant-local monotonic delivery sequence used by read cursors.
 */
public class DeliveredMessage {

    private long deliverySeq;
    private Message message;

    public DeliveredMessage() {
    }

    public DeliveredMessage(long deliverySeq, Message message) {
        this.deliverySeq = deliverySeq;
        this.message = message;
    }

    public long getDeliverySeq() {
        return deliverySeq;
    }

    public void setDeliverySeq(long deliverySeq) {
        this.deliverySeq = deliverySeq;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
