// SPDX-License-Identifier: Apache-2.0
package com.hedera.node.app.blocks.impl.streaming;

public class ConnectionState {
    private Status status;
    private String message;

    public void setStatus(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public enum Status {
        UNINITIALIZED,
        CONNECTING,
        ACTIVE,
        RECEIVED_END_OF_STREAM,
        RECEIVED_BLOCK_ACK,
        RECEIVED_SKIP_BLOCK,
        RECEIVED_RESEND_BLOCK,
        FAILED,
        RECONNECTING,
        CLOSED
    }

    public ConnectionState() {
        this.status = Status.UNINITIALIZED;
        this.message = "";
    }
}
