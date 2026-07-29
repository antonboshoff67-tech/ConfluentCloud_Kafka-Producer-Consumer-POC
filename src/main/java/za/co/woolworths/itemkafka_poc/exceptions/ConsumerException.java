package za.co.woolworths.itemkafka_poc.exceptions;

public class ConsumerException extends Exception {
    private String detailedMessage;

    /** @param message */
    public ConsumerException(final String message) {
        super(message);
        this.detailedMessage = message;
    }

    /** @param cause */
    public ConsumerException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public ConsumerException(final String message, final Throwable cause) {
        super(message, cause);
        this.detailedMessage = message;
    }

    public String getDetailedMessage() {
        return detailedMessage;
    }
}
