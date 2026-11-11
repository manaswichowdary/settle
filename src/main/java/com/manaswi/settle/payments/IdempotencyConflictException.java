package com.manaswi.settle.payments;

/** The same idempotency key was sent again with a different request body. */
public class IdempotencyConflictException extends RuntimeException {
    public IdempotencyConflictException(String key) {
        super("Idempotency key " + key + " was already used for a different request");
    }
}
