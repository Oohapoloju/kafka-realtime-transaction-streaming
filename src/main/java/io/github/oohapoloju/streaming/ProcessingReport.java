package io.github.oohapoloju.streaming;

import java.math.BigDecimal;
import java.util.Map;

/** Snapshot of processing statistics returned by the reporting endpoint. */
public record ProcessingReport(
    long processed, long duplicatesIgnored, Map<String, BigDecimal> totalsByAccount) {}
