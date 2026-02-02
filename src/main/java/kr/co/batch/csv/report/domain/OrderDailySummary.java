package kr.co.batch.csv.report.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDate;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Entity
@Table(name = "order_daily_summary")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderDailySummary {

    @Id
    @Column(nullable = false)
    private LocalDate orderDate;

    private Long totalAmountKrw;

    private Long totalOrderCount;

    private Long totalItemCount;

    public static OrderDailySummary create(final LocalDate orderDate, final Long amountKrw, final Long itemCount) {
        OrderDailySummary summary = new OrderDailySummary();

        summary.orderDate = orderDate;
        summary.totalAmountKrw = amountKrw;
        summary.totalOrderCount = 1L;
        summary.totalItemCount = itemCount;

        return summary;
    }
}
