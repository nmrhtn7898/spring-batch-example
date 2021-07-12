package com.nuguri.springbatchexample.writer;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ElasticsearchItemWriter<T extends IndexRequest> implements ItemWriter<T>, InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final RestHighLevelClient restHighLevelClient;

    public ElasticsearchItemWriter(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        if (CollectionUtils.isEmpty(items)) {
            logger.warn("No items to write to elasticsearch. list is empty or null");
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(new TimeValue(1L, TimeUnit.MINUTES));
        items.forEach(bulkRequest::add);
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            throw new ElasticsearchException("Failed write items");
        }
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(restHighLevelClient, "RestHighLevelClient is must be not null");
    }

}
