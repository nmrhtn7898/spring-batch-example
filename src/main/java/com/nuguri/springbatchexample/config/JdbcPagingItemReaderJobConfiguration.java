package com.nuguri.springbatchexample.config;

import com.nuguri.springbatchexample.entity.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
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
@RequiredArgsConstructor
@Configuration
public class JdbcPagingItemReaderJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final DataSource dataSource;

    private static final int CHUNK_SIZE = 10;

    @Bean
    public Job jdbcPagingItemReaderJob() throws Exception {
        return jobBuilderFactory
                .get("jdbcPagingItemReaderJob")
                .start(jdbcPagingItemReaderStep())
                .build();
    }

    @Bean
    public Step jdbcPagingItemReaderStep() throws Exception {
        return stepBuilderFactory
                .get("jdbcPagingItemReaderStep")
                .<Pay, Pay>chunk(CHUNK_SIZE)
                .reader(jdbcPagingItemReader())
                .writer(jdbcPagingItemWriter())
                .build();
    }

    @Bean
    public JdbcPagingItemReader<Pay> jdbcPagingItemReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("amount", 2000);
        return new JdbcPagingItemReaderBuilder<Pay>()
                .pageSize(CHUNK_SIZE)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
                .queryProvider(pagingQueryProvider())
                .parameterValues(parameterValues)
                .name("jdbcPagingItemReader")
                .build();
    }

    private ItemWriter<Pay> jdbcPagingItemWriter() {
        return list -> {
            for (Pay pay : list) {
                log.info("Current Pay={}", pay);
            }
        };
    }

    @Bean
    public PagingQueryProvider pagingQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean sqlPagingQueryProviderFactoryBean =
                new SqlPagingQueryProviderFactoryBean();
        sqlPagingQueryProviderFactoryBean.setDataSource(dataSource);
        sqlPagingQueryProviderFactoryBean.setSelectClause("id, amount, tx_name, tx_date_time");
        sqlPagingQueryProviderFactoryBean.setFromClause("from pay");
        sqlPagingQueryProviderFactoryBean.setWhereClause("where amount >= :amount");
        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);
        sqlPagingQueryProviderFactoryBean.setSortKeys(sortKeys);
        return sqlPagingQueryProviderFactoryBean.getObject();
    }

}
