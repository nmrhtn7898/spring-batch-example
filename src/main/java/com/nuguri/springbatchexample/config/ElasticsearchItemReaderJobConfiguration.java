package com.nuguri.springbatchexample.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nuguri.springbatchexample.entity.Pay2;
import com.nuguri.springbatchexample.reader.ElasticsearchNoOffsetPagingItemReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ElasticsearchItemReaderJobConfiguration {

    public static final String JOB_PREFIX = "elasticsearch2";

    public static final String JOB_NAME = JOB_PREFIX + "ItemWriteJob";

    public static final String STEP_NAME = JOB_PREFIX + "ItemWriteStep";

    public static final String READER_NAME = JOB_PREFIX + "ItemReader";

    public static final String WRITER_NAME = JOB_NAME + "ItemWriter";

    private static final int CHUNK_SIZE = 10;

    private final RestHighLevelClient restHighLevelClient;

    private final ObjectMapper objectMapper;

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final DataSource dataSource;

    @Bean(JOB_NAME)
    public Job elasticsearchItemWriterJob() {
        return jobBuilderFactory
                .get(JOB_NAME)
                .start(elasticsearchItemWriterStep())
                .build();
    }

    @Bean(JOB_PREFIX + STEP_NAME)
    public Step elasticsearchItemWriterStep() {
        return stepBuilderFactory
                .get(JOB_PREFIX + STEP_NAME)
                .<SearchHit, Pay2>chunk(CHUNK_SIZE)
                .reader(elasticsearchNoOffsetPagingItemReader())
                .processor(processor())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    @Bean(JOB_PREFIX + READER_NAME)
    public ElasticsearchNoOffsetPagingItemReader<SearchHit> elasticsearchNoOffsetPagingItemReader() {
        return new ElasticsearchNoOffsetPagingItemReader<>(
                restHighLevelClient,
                CHUNK_SIZE,
                searchSourceBuilder(),
                "pay"
        );
    }

    @Bean(JOB_PREFIX + WRITER_NAME)
    public JdbcBatchItemWriter<Pay2> jdbcBatchItemWriter() {
        return new JdbcBatchItemWriterBuilder<Pay2>()
                .dataSource(dataSource)
                .sql("insert into pay2(amount, tx_name, tx_date_time) values (:amount, :txName, :txDateTime)")
                .beanMapped()
                .build();
    }

    private SearchSourceBuilder searchSourceBuilder() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort("amount");
        return searchSourceBuilder;
    }

    private ItemProcessor<SearchHit, Pay2> processor() {
        return searchHit -> objectMapper.readValue(searchHit.getSourceAsString(), Pay2.class);
    }

}
