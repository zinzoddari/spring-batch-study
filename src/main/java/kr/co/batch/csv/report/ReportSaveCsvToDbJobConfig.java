package kr.co.batch.csv.report;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import javax.sql.DataSource;
import kr.co.batch.csv.report.domain.Order;
import kr.co.batch.csv.report.domain.OrderDailySummary;
import kr.co.batch.csv.report.dto.ReportCsvDto;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class ReportSaveCsvToDbJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final DataSource dataSource;

    @Bean
    public Job reportSaveCsvToDbJob() {
        return new JobBuilder("reportSaveCsvToDbJob", jobRepository)
            .start(reportSaveCsvToDbStep())
            .next(reportSummuryStep())
            .build();
    }

    @Bean
    public Step reportSaveCsvToDbStep() {
        return new StepBuilder("reportSaveCsvToDbStep", jobRepository)
            .<ReportCsvDto, Order>chunk(2, platformTransactionManager)
            .reader(reportSaveCsvToDbReader())
            .processor(reportSaveCsvToDbProcessor())
            .writer(reportSaveCsvToDbWriter())
            .build();
    }

    @Bean
    public Step reportSummuryStep() {
        return new StepBuilder("reportSummuryStep", jobRepository)
            .<Order, OrderDailySummary>chunk(3, platformTransactionManager)
            .reader(reportSummaryReader())
            .processor(reportSummaryProcessor())
            .writer(reportSummaryWriter())
            .build();
    }

    @Bean
    public ItemReader<ReportCsvDto> reportSaveCsvToDbReader() {
        return new FlatFileItemReaderBuilder<ReportCsvDto>()
            .name("reportSaveCsvToDbReader")
            .resource(new ClassPathResource("orders_2026-02-02.csv"))
            .linesToSkip(1)
            .delimited()
            .names("order_id", "user_id", "order_datetime", "amount", "currency", "status", "item_count")
            .fieldSetMapper(it -> new ReportCsvDto(
                it.readString("order_id"),
                it.readLong("user_id"),
                LocalDateTime.parse(it.readString("order_datetime"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                it.readLong("amount"),
                it.readString("currency"),
                it.readString("status"),
                it.readLong("item_count")
            ))
            .build();
    }

    @Bean
    public ItemProcessor<ReportCsvDto, Order> reportSaveCsvToDbProcessor() {
        return item -> item.orderId() == null ? null : Order.create(item);
    }

    @Bean
    public ItemWriter<Order> reportSaveCsvToDbWriter() {
        return new JdbcBatchItemWriterBuilder<Order>()
            .dataSource(dataSource)
            .sql("""
                    INSERT INTO order_tx (order_id, user_id, order_datetime, order_date, amount_krw, item_count)
                    VALUES(:orderId, :userId, :orderDatetime, :orderDate, :amountKrw, :itemCount)
                    ON DUPLICATE KEY UPDATE
                        user_id = VALUES(user_id),
                        order_datetime = VALUES(order_datetime),
                        order_date = VALUES(order_date),
                        amount_krw = VALUES(amount_krw),
                        item_count = VALUES(item_count)
                """)
            .beanMapped()
            .build();
    }

    @Bean
    public ItemReader<Order> reportSummaryReader() {
        return new JdbcCursorItemReaderBuilder<Order>()
            .name("reportSummuryReader")
            .dataSource(dataSource)
            .sql("""
                    SELECT order_id as orderId,
                           user_id as userId,
                           order_datetime as orderDatetime,
                           order_date as orderDate,
                           amount_krw as amountKrw,
                           item_count as itemCount
                    FROM order_tx
                    WHERE order_date = '2026-02-02'
                """)  // TODO: order_date 이거 외부로 어떻게 받을 수 있을까
            .rowMapper((rs, rowNum) -> Order.create(
                rs.getString("orderId"),
                rs.getLong("userId"),
                LocalDateTime.parse(rs.getString("orderDatetime"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                LocalDate.parse(rs.getString("orderDate"), DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                rs.getLong("amountKrw"),
                rs.getLong("itemCount")
            ))
            .fetchSize(1000) // ?
            .build();
    }

    @Bean
    public ItemProcessor<Order, OrderDailySummary> reportSummaryProcessor() {
        return item -> OrderDailySummary.create(item.getOrderDate(), item.getAmountKrw(), item.getItemCount());
    }

    @Bean
    public ItemWriter<OrderDailySummary> reportSummaryWriter() {
        return new JdbcBatchItemWriterBuilder<OrderDailySummary>()
            .dataSource(dataSource)
            .sql("""
                INSERT INTO order_daily_summary (order_date, total_amount_krw, total_order_count, total_item_count)
                VALUES (:orderDate, :totalAmountKrw, :totalOrderCount, :totalItemCount)
                ON DUPLICATE KEY UPDATE
                    total_amount_krw = total_amount_krw + VALUES(total_amount_krw),
                    total_order_count = total_order_count + VALUES(total_order_count),
                    total_item_count = total_item_count + VALUES(total_item_count)
                """)
            .beanMapped()
            .build();
    }
}
