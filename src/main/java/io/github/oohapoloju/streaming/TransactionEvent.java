package io.github.oohapoloju.streaming;

import java.math.BigDecimal;
import java.time.Instant;

/** Immutable transaction event published to the transactions topic. */
public record TransactionEvent(
    String transactionId,
    String accountId,
    BigDecimal amount,
    String currency,
    Instant occurredAt) {}
