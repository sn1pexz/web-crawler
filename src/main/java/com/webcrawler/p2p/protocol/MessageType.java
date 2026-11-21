package com.webcrawler.p2p.protocol;

public enum MessageType {
    HELLO,
    JOIN,
    TASK_REQUEST,
    TASK_BATCH,
    RESULT,
    HEARTBEAT,
    LEAVE,
    PAUSE,
    RESUME,
    STOP
}
