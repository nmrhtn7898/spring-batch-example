package com.nuguri.springbatchexample.reader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.AbstractPagingItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

public class ElasticsearchNoOffsetPagingItemReader<T> extends AbstractPagingItemReader<T> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final RestHighLevelClient restHighLevelClient;

    private final SearchSourceBuilder searchSourceBuilder;

    private final String indexName;

    private Object[] sortValues;

    public ElasticsearchNoOffsetPagingItemReader(RestHighLevelClient restHighLevelClient, int pageSize,
                                                 SearchSourceBuilder searchSourceBuilder, String indexName) {
        setName(ClassUtils.getShortName(getClass()));
        setPageSize(pageSize);
        this.restHighLevelClient = restHighLevelClient;
        this.searchSourceBuilder = searchSourceBuilder;
        this.indexName = indexName;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doReadPage() {
        SearchRequest searchRequest = createQuery();
        initResults();
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                sortValues = hit.getSortValues();
                results.add((T) hit);
            }
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    private SearchRequest createQuery() {
        if (sortValues != null && sortValues.length > 0) {
            searchSourceBuilder.searchAfter(sortValues);
        }
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchSourceBuilder.size(getPageSize());
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Assert.notNull(restHighLevelClient, "restHighLevelClient is must be not null");
        Assert.notNull(searchSourceBuilder, "searchSourceBuilder is must be not null");
        Assert.hasText(indexName, "index name is must be has text");
        Assert.notEmpty(searchSourceBuilder.sorts(), "sort key is must be not empty");
    }

    @Override
    protected void doJumpToPage(int itemIndex) {

    }

    protected void initResults() {
        if (CollectionUtils.isEmpty(results)) {
            results = new CopyOnWriteArrayList<>();
        } else {
            results.clear();
        }
    }

}
