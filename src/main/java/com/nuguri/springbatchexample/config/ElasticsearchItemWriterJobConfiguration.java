package com.nuguri.springbatchexample.config;

import com.nuguri.springbatchexample.entity.Pay;
import com.nuguri.springbatchexample.writer.ElasticsearchItemWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ElasticsearchItemWriterJobConfiguration {

    public static final String JOB_PREFIX = "elasticsearch";

    public static final String JOB_NAME = JOB_PREFIX + "ItemWriteJob";

    public static final String STEP_NAME = JOB_PREFIX + "ItemWriteStep";

    public static final String READER_NAME = JOB_PREFIX + "ItemReader";

    public static final String WRITER_NAME = JOB_NAME + "ItemWriter";

    private static final int CHUNK_SIZE = 10;

    private final RestHighLevelClient restHighLevelClient;

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final DataSource dataSource;

    @Bean(JOB_NAME)
    public Job elasticsearchItemWriterJob() throws Exception {
        return jobBuilderFactory
                .get(JOB_NAME)
                .start(elasticsearchItemWriterStep())
                .build();
    }

    @Bean(JOB_PREFIX + STEP_NAME)
    public Step elasticsearchItemWriterStep() throws Exception {
        return stepBuilderFactory
                .get(JOB_PREFIX + STEP_NAME)
                .<Pay, IndexRequest>chunk(CHUNK_SIZE)
                .reader(jdbcPagingItemReader())
                .processor(processor())
                .writer(elasticsearchItemWriter())
                .build();
    }

    @Bean(JOB_PREFIX + READER_NAME)
    public JdbcPagingItemReader<Pay> jdbcPagingItemReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Pay>()
                .pageSize(CHUNK_SIZE)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
                .queryProvider(pagingQueryProvider())
                .name(JOB_PREFIX + READER_NAME)
                .build();
    }

    @Bean(JOB_PREFIX + WRITER_NAME)
    public ElasticsearchItemWriter<IndexRequest> elasticsearchItemWriter() {
        return new ElasticsearchItemWriter<>(restHighLevelClient);
    }

    @Bean(JOB_PREFIX + "pagingQueryProvider")
    public PagingQueryProvider pagingQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean sqlPagingQueryProviderFactoryBean = new SqlPagingQueryProviderFactoryBean();
        sqlPagingQueryProviderFactoryBean.setDataSource(dataSource);
        sqlPagingQueryProviderFactoryBean.setSelectClause("id, amount, tx_name, tx_date_time");
        sqlPagingQueryProviderFactoryBean.setFromClause("from pay");
        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);
        sqlPagingQueryProviderFactoryBean.setSortKeys(sortKeys);
        return sqlPagingQueryProviderFactoryBean.getObject();
    }

    private ItemProcessor<Pay, IndexRequest> processor() {
        return item -> {
            IndexRequest indexRequest = new IndexRequest("pay");
            indexRequest.id(item.getId().toString());
            indexRequest.source(
                    "amount", item.getAmount(),
                    "txName", item.getTxName(),
                    "txDateTime", item.getTxDateTime().toString()
            );
            return indexRequest;
        };
    }

}
