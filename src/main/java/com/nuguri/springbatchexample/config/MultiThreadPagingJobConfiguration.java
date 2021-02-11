package com.nuguri.springbatchexample.config;

import com.nuguri.springbatchexample.entity.Pay;
import com.nuguri.springbatchexample.entity.Pay2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class MultiThreadPagingJobConfiguration {

    public static final String JOB_NAME = "multiThreadPagingBatch";

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    @Value("${chunkSize:1000}")
    private int chunkSize;

    @Value("${poolSize:10}")
    private int poolSize;

    @Bean(name = JOB_NAME + "taskPool")
    public TaskExecutor executor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor(); // (2)
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setThreadNamePrefix("multi-thread-");
        executor.setWaitForTasksToCompleteOnShutdown(Boolean.TRUE);
        executor.initialize();
        return executor;
    }

    @Bean(name = JOB_NAME)
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(step())
                .preventRestart()
                .build();
    }

    @Bean(name = JOB_NAME + "_step")
    @JobScope
    public Step step() {
        return stepBuilderFactory.get(JOB_NAME + "_step")
                .<Pay, Pay2>chunk(chunkSize)
                .reader(reader(null))
                .processor(processor())
                .writer(writer())
                .taskExecutor(executor())
                .throttleLimit(poolSize)
                .build();
    }

    @Bean(JOB_NAME + "_reader")
    @StepScope
    public JpaPagingItemReader<Pay> reader(@Value("#{jobParameters[txDateTime]}") String txDateTime) {
        return new JpaPagingItemReaderBuilder<Pay>()
                .name(JOB_NAME + "_reader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(chunkSize)
                .queryString("SELECT p FROM Pay p WHERE p.txDateTime =:txDateTime")
                .parameterValues(Collections.singletonMap("txDateTime", LocalDateTime.parse(txDateTime)))
                .saveState(false)
                .build();
    }

    private ItemProcessor<Pay, Pay2> processor() {
        return pay -> {
            Pay2 pay2 = new Pay2();
            pay2.setTxName(pay.getTxName());
            pay2.setAmount(pay.getAmount());
            pay2.setTxDateTime(pay.getTxDateTime());
            return pay2;
        };
    }

    @Bean(JOB_NAME + "_writer")
    @StepScope
    public JpaItemWriter<Pay2> writer() {
        return new JpaItemWriterBuilder<Pay2>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

}
