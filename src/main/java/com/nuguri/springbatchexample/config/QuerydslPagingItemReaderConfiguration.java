package com.nuguri.springbatchexample.config;

import com.nuguri.springbatchexample.reader.QuerydslPagingItemReader;
import com.nuguri.springbatchexample.entity.Pay;
import com.nuguri.springbatchexample.entity.Pay2;
import com.nuguri.springbatchexample.entity.QPay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class QuerydslPagingItemReaderConfiguration {

    public static final String JOB_NAME = "querydslPagingReaderJob";

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    @Value("${chunkSize:1000}")
    private int chunkSize;

    @Bean(JOB_NAME)
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(step())
                .build();
    }

    public Step step() {
        return stepBuilderFactory.get("querydslPagingReaderStep")
                .<Pay, Pay2>chunk(chunkSize)
                .reader(querydslReader())
                .processor(processor())
                .writer(querydslWriter())
                .build();
    }

    public QuerydslPagingItemReader<Pay> querydslReader() {
        return new QuerydslPagingItemReader<>(
                entityManagerFactory,
                chunkSize,
                jpaQueryFactory -> jpaQueryFactory.selectFrom(QPay.pay)
        );
    }

    private ItemProcessor<Pay, Pay2> processor() {
        return item -> {
            Pay2 pay2 = new Pay2();
            pay2.setTxDateTime(item.getTxDateTime());
            pay2.setAmount(item.getAmount());
            pay2.setTxName(item.getTxName());
            return pay2;
        };
    }

    public JpaItemWriter<Pay2> querydslWriter() {
        return new JpaItemWriterBuilder<Pay2>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

}
