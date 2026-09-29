package com.github.guoyyit.causalbroadcastjava.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Page of delivered messages returned to a pulling receiver.
 */
public class PullResult {

    private long cursor;
    private List<DeliveredMessage> messages = new ArrayList<>();

    public PullResult() {
    }

    public PullResult(long cursor, List<DeliveredMessage> messages) {
        this.cursor = cursor;
        this.messages = messages;
    }

    public long getCursor() {
        return cursor;
    }

    public void setCursor(long cursor) {
        this.cursor = cursor;
    }

    public List<DeliveredMessage> getMessages() {
        return messages;
    }

    public void setMessages(List<DeliveredMessage> messages) {
        this.messages = messages;
    }
}
