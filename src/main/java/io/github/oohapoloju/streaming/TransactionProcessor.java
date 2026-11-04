package io.github.oohapoloju.streaming;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes transaction events and keeps running totals per account.
 * Processing is idempotent: a replayed or duplicated event (same transactionId) is ignored,
 * which makes the consumer safe under Kafka's at-least-once delivery.
 */
@Component
public class TransactionProcessor {

  private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

  private final Map<String, Boolean> seen = new ConcurrentHashMap<>();
  private final Map<String, BigDecimal> totalsByAccount = new ConcurrentHashMap<>();
  private final AtomicLong processed = new AtomicLong();
  private final AtomicLong duplicates = new AtomicLong();

  @KafkaListener(topics = TransactionController.TOPIC, groupId = "reporting")
  public void onEvent(TransactionEvent event) {
    if (seen.putIfAbsent(event.transactionId(), Boolean.TRUE) != null) {
      duplicates.incrementAndGet();
      log.debug("Skipping duplicate transaction {}", event.transactionId());
      return;
    }
    totalsByAccount.merge(event.accountId(), event.amount(), BigDecimal::add);
    processed.incrementAndGet();
  }

  public ProcessingReport report() {
    return new ProcessingReport(processed.get(), duplicates.get(), Map.copyOf(totalsByAccount));
  }
}
