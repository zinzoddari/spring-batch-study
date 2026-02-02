package kr.co.batch.csv.user;

import javax.sql.DataSource;
import kr.co.batch.csv.user.domain.User;
import kr.co.batch.csv.user.dto.UserCsv;
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
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
class UserInitJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final DataSource dataSource;

    @Bean
    public Job userInitCsvToDbJob() {
        return new JobBuilder("userInitCsvToDbJob", jobRepository)
            .start(userInitStep())
            .build();
    }

    @Bean
    public Step userInitStep() {
        return new StepBuilder("userInitStep", jobRepository)
            .<UserCsv, User>chunk(2, platformTransactionManager)
            .reader(userInitReaderByCsv())
            .processor(userInitProcessor())
            .writer(userInitWriterToDb())
            .build();
    }

    @Bean
    public ItemReader<UserCsv> userInitReaderByCsv() {
        return new FlatFileItemReaderBuilder<UserCsv>()
            .name("userInitReaderByCsv")
            .resource(new ClassPathResource("users.csv"))
            .linesToSkip(1)
            .delimited()
            .names("id", "name", "age")
            .fieldSetMapper(it -> new UserCsv(
                it.readLong("id"),
                it.readString("name"),
                it.readInt("age")
            )).build();
    }

    @Bean
    public ItemProcessor<UserCsv, User> userInitProcessor() {
        return item -> item.age() > 18 ? User.create(item.id(), item.name(), item.age()) : null;
    }

    @Bean
    public ItemWriter<User> userInitWriterToDb() {
        return new JdbcBatchItemWriterBuilder<User>()
            .dataSource(dataSource)
            .sql("""
                INSERT INTO user_info (id, name, age)
                VALUES (:id, :name, :age)
                """)
            .beanMapped()
            .build();
    }
}
