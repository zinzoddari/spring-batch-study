package kr.co.batch.csv.report.dto;

import java.time.LocalDateTime;

public record ReportCsvDto(
    String orderId,
    Long userId,
    LocalDateTime orderDateTime,
    Long amount,
    String currency,
    String status,
    Long itemCount
) {
}
