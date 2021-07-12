package com.nuguri.springbatchexample;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchExampleApplication {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper;
    }

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("http://localhost:9200"));
        return new RestHighLevelClient(restClientBuilder);
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(SpringBatchExampleApplication.class, args);
        SpringApplication.exit(run);
    }

}
