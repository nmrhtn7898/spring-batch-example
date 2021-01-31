package com.nuguri.springbatchexample.config;

import com.nuguri.springbatchexample.entity.Pay;
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
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;
import java.util.Arrays;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class ProcessorCompositeJobConfiguration {

    public static final String JOB_NAME = "processorCompositeBatch";

    public static final String BEAN_PREFIX = JOB_NAME + "_";

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    @Value("${chunkSize:1000}")
    private int chunkSize;

    @Bean(JOB_NAME)
    public Job job() {
        return jobBuilderFactory
                .get(JOB_NAME)
                .preventRestart()
                .start(step())
                .build();
    }

    public Step step() {
        return stepBuilderFactory
                .get(BEAN_PREFIX + "step")
                .<Pay, String>chunk(chunkSize)
                .reader(reader())
                .processor(compositeItemProcessor())
                .writer(writer())
                .build();
    }

    public JpaPagingItemReader<Pay> reader() {
        return new JpaPagingItemReaderBuilder<Pay>()
                .name(BEAN_PREFIX + "reader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(chunkSize)
                .queryString("SELECT p FROM Pay p")
                .build();
    }

    public CompositeItemProcessor compositeItemProcessor() {
        CompositeItemProcessor processor = new CompositeItemProcessor<>();
        processor.setDelegates(Arrays.asList(processor(), processor2()));
        return processor;
    }

    public ItemProcessor<Pay, String> processor() {
        return Pay::getTxName;
    }

    public ItemProcessor<String, String> processor2() {
        return name -> "안녕하세요. " + name + "입니다.";
    }

    private ItemWriter<String> writer() {
        return items -> items.forEach(item -> log.info("Pay txName={}", item));
    }

}
