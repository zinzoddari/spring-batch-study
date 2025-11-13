package kr.co.batch.hello;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
class SimpleJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    Job simpleJob() {
        return new JobBuilder("sampleJob", jobRepository)
            .start(simpleHelloStep())
            .build();
    }

    @Bean
    Step simpleHelloStep() {
        return new StepBuilder("simpleHelloStep", jobRepository)
            .tasklet((contribution, chunkContext) -> {
                System.out.println("Hello World");

                return RepeatStatus.FINISHED;
            }, platformTransactionManager)
            .build();
    }
}
