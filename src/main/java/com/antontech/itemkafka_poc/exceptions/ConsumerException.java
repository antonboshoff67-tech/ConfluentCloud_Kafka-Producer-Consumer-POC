package com.antontech.itemkafka_poc.exceptions;

/** Checked exception raised when the gateway/Kafka message-routing pipeline fails to process a request. */
public class ConsumerException extends Exception {
    private String detailedMessage;

    /** @param message human-readable failure reason. */
    public ConsumerException(final String message) {
        super(message);
        this.detailedMessage = message;
    }

    /** @param cause the underlying cause of the failure. */
    public ConsumerException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message human-readable failure reason.
     * @param cause   the underlying cause of the failure.
     */
    public ConsumerException(final String message, final Throwable cause) {
        super(message, cause);
        this.detailedMessage = message;
    }

    /** @return the human-readable failure reason supplied at construction time. */
    public String getDetailedMessage() {
        return detailedMessage;
    }
}

