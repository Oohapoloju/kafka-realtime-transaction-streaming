package io.github.oohapoloju.streaming;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TransactionProcessorTest {

  private static TransactionEvent event(String id, String account, String amount) {
    return new TransactionEvent(id, account, new BigDecimal(amount), "USD", Instant.parse("2026-01-01T10:00:00Z"));
  }

  @Test
  void accumulatesTotalsPerAccount() {
    TransactionProcessor processor = new TransactionProcessor();

    processor.onEvent(event("t1", "acc-1", "10.50"));
    processor.onEvent(event("t2", "acc-1", "4.50"));
    processor.onEvent(event("t3", "acc-2", "7.00"));

    ProcessingReport report = processor.report();
    assertThat(report.processed()).isEqualTo(3);
    assertThat(report.totalsByAccount().get("acc-1")).isEqualByComparingTo("15.00");
    assertThat(report.totalsByAccount().get("acc-2")).isEqualByComparingTo("7.00");
  }

  @Test
  void ignoresDuplicateTransactionIds() {
    TransactionProcessor processor = new TransactionProcessor();

    processor.onEvent(event("t1", "acc-1", "10.00"));
    processor.onEvent(event("t1", "acc-1", "10.00")); // redelivered by Kafka

    ProcessingReport report = processor.report();
    assertThat(report.processed()).isEqualTo(1);
    assertThat(report.duplicatesIgnored()).isEqualTo(1);
    assertThat(report.totalsByAccount().get("acc-1")).isEqualByComparingTo("10.00");
  }
}
