package com.nuguri.springbatchexample.job;

import com.nuguri.springbatchexample.entity.Pay;
import com.nuguri.springbatchexample.entity.Pay2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class CustomItemWriterJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    private static final int CHUNK_SIZE = 10;

    @Bean
    public Job customItemWriterJob() {
        return jobBuilderFactory
                .get("customItemWriterJob")
                .start(customItemWriterStep())
                .build();
    }

    @Bean
    public Step customItemWriterStep() {
        return stepBuilderFactory
                .get("customItemWriterStep")
                .<Pay, Pay2>chunk(CHUNK_SIZE)
                .reader(customItemReader())
                .processor(customItemWriterProcessor())
                .writer(customItemWriter())
                .build();
    }

    @Bean
    public JpaPagingItemReader<Pay> customItemReader() {
        return new JpaPagingItemReaderBuilder<Pay>()
                .name("customItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(CHUNK_SIZE)
                .queryString("SELECT p FROM Pay p")
                .build();
    }

    @Bean
    public ItemProcessor<Pay, Pay2> customItemWriterProcessor() {
        return pay -> {
            Pay2 pay2 = new Pay2();
            pay2.setAmount(pay.getAmount());
            pay2.setTxName(pay.getTxName());
            pay2.setTxDateTime(pay.getTxDateTime());
            return pay2;
        };
    }

    @Bean
    public ItemWriter<Pay2> customItemWriter() {
        return items -> items.forEach(System.out::println);
    }

}
