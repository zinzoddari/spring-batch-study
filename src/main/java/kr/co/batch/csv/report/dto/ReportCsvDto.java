package kr.co.batch.csv.report.dto;

public record ReportCsvDto(
    String orderId,
    Long userId,
    String orderDateTime,
    Long amount,
    String currency,
    String status,
    Long itemCount
) {
}
