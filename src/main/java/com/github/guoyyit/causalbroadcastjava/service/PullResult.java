package com.github.guoyyit.causalbroadcastjava.service;

import com.github.guoyyit.causalbroadcastjava.model.BroadcastMessage;

import java.util.List;

/**
 * Result of a delivered-log pull: the page of messages and the cursor to
 * continue with on the next pull.
 */
public record PullResult(List<BroadcastMessage> messages, long nextSeq) {
}
