package kr.co.batch.csv.report;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import javax.sql.DataSource;
import kr.co.batch.csv.report.domain.Order;
import kr.co.batch.csv.report.domain.OrderDailySummary;
import kr.co.batch.csv.report.dto.ReportCsvDto;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
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
    public Job orderImportJob(final Step importOrderTxStep, final Step dailySummaryStep) {
        return new JobBuilder("orderImportJob", jobRepository)
            .start(importOrderTxStep)
            .next(dailySummaryStep)
            .build();
    }

    @Bean
    public Step importOrderTxStep(
        final FlatFileItemReader<ReportCsvDto> reportSaveCsvToDbReader,
        final ItemProcessor<ReportCsvDto, Order> reportSaveCsvToDbProcessor,
        final ItemWriter<Order> reportSaveCsvToDbWriter
    ) {
        return new StepBuilder("importOrderTxStep", jobRepository)
            .<ReportCsvDto, Order>chunk(2, platformTransactionManager)
            .reader(reportSaveCsvToDbReader)
            .processor(reportSaveCsvToDbProcessor)
            .writer(reportSaveCsvToDbWriter)
            .build();
    }

    @Bean
    public Step dailySummaryStep(
        final JdbcCursorItemReader<OrderDailySummary> reportSummaryReader,
        final ItemWriter<OrderDailySummary> reportSummaryWriter
    ) {
        return new StepBuilder("dailySummaryStep", jobRepository)
            .<OrderDailySummary, OrderDailySummary>chunk(1, platformTransactionManager)
            .reader(reportSummaryReader)
            .writer(reportSummaryWriter)
            .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<ReportCsvDto> reportSaveCsvToDbReader(@Value("#{jobParameters['targetDate']}") final String targetDate) {
        return new FlatFileItemReaderBuilder<ReportCsvDto>()
            .name("reportSaveCsvToDbReader")
            .resource(new ClassPathResource(String.format("orders_%s.csv", targetDate)))
            .linesToSkip(1)
            .delimited()
            .names("order_id", "user_id", "order_datetime", "amount", "currency", "status", "item_count")
            .fieldSetMapper(it -> new ReportCsvDto(
                it.readString("order_id"),
                it.readLong("user_id"),
                it.readString("order_datetime"),
                it.readLong("amount"),
                it.readString("currency"),
                it.readString("status"),
                it.readLong("item_count")
            ))
            .build();
    }

    @Bean
    public ItemProcessor<ReportCsvDto, Order> reportSaveCsvToDbProcessor() {
        return item -> {
            if (!"PAID".equals(item.status())) {
                return null;
            }

            if (item.amount() <= 0) {
                return null;
            }

            if (item.itemCount() <= 0) {
                return null;
            }

            if (!"KRW".equals(item.currency()) && !"USD".equals(item.currency())) {
                return null;
            }

            LocalDateTime dateTime;

            try {
                dateTime = LocalDateTime.parse(item.orderDateTime(),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            } catch (DateTimeParseException e) {
                return null;
            }

            final Long amountKrw = "USD".equals(item.currency()) ? item.amount() * 1300 : item.amount();

            return Order.create(
                item.orderId(),
                item.userId(),
                dateTime,
                dateTime.toLocalDate(),
                amountKrw,
                item.itemCount()
            );
        };
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
    @StepScope
    public JdbcCursorItemReader<OrderDailySummary> reportSummaryReader(@Value("#{jobParameters['targetDate']}") final String targetDate) {
        return new JdbcCursorItemReaderBuilder<OrderDailySummary>()
            .name("reportSummaryReader")
            .dataSource(dataSource)
            .sql("""
                    select
                        order_date as orderDate,
                        sum(amount_krw) as totalAmountKrw,
                        count(order_id) as totalOrderCount,
                        sum(item_count) as totalItemCount
                    from order_tx
                    where order_date = ?
                    group by order_date
                """)
            .preparedStatementSetter(ps ->
                ps.setDate(1, Date.valueOf(targetDate))
            )
            .rowMapper((rs, rowNum) -> OrderDailySummary.create(
                rs.getDate("orderDate").toLocalDate(),
                rs.getLong("totalAmountKrw"),
                rs.getLong("totalOrderCount"),
                rs.getLong("totalItemCount")
            ))
            .build();
    }

    @Bean
    public ItemWriter<OrderDailySummary> reportSummaryWriter() {
        return new JdbcBatchItemWriterBuilder<OrderDailySummary>()
            .dataSource(dataSource)
            .sql("""
                INSERT INTO order_daily_summary (order_date, total_amount_krw, total_order_count, total_item_count)
                VALUES (:orderDate, :totalAmountKrw, :totalOrderCount, :totalItemCount)
                ON DUPLICATE KEY UPDATE
                    total_amount_krw = VALUES(total_amount_krw),
                    total_order_count = VALUES(total_order_count),
                    total_item_count = VALUES(total_item_count)
                """)
            .beanMapped()
            .build();
    }
}
