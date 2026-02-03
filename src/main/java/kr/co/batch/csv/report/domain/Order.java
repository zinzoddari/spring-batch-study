package kr.co.batch.csv.report.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Entity
@Table(name = "order_tx")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order {

    @Id
    @Column(name = "order_id", nullable = false)
    private String orderId;

    private Long userId;

    private LocalDateTime orderDatetime;

    private LocalDate orderDate;

    private Long amountKrw;

    private Long itemCount;

    public static Order create(final String orderId, final Long userId, final LocalDateTime orderDatetime, final LocalDate orderDate, final Long amountKrw, final Long itemCount) {
        Order order = new Order();

        order.orderId = orderId;
        order.userId = userId;
        order.orderDatetime = orderDatetime;
        order.orderDate = orderDate;
        order.amountKrw = amountKrw;
        order.itemCount = itemCount;

        return order;
    }
}
