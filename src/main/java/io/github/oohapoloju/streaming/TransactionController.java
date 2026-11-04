package io.github.oohapoloju.streaming;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** REST entry point: publishes transactions to Kafka and exposes live reporting stats. */
@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

  public static final String TOPIC = "transactions";

  private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
  private final TransactionProcessor processor;

  public TransactionController(
      KafkaTemplate<String, TransactionEvent> kafkaTemplate, TransactionProcessor processor) {
    this.kafkaTemplate = kafkaTemplate;
    this.processor = processor;
  }

  @PostMapping
  public ResponseEntity<String> publish(@RequestBody TransactionEvent event) {
    // Keyed by account so all events for one account stay ordered in one partition.
    kafkaTemplate.send(TOPIC, event.accountId(), event);
    return ResponseEntity.accepted().body("queued " + event.transactionId());
  }

  @GetMapping("/report")
  public ProcessingReport report() {
    return processor.report();
  }
}
