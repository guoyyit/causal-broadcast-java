package com.github.guoyyit.causalbroadcastjava.core;

/**
 * Outcome of injecting a message into a receiver.
 */
public enum ReceiveStatus {
    DELIVERED,
    BUFFERED,
    DUPLICATE,
    REJECTED
}
