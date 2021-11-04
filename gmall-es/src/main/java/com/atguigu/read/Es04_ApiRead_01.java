package com.atguigu.read;


import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;


import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

import org.elasticsearch.search.aggregations.AggregationBuilders;

import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Es04_ApiRead_01 {
    public static void main(String[] args) throws IOException {
        //  创建工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        // 设置连接对象
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        // 获取连接
        JestClient jestClient = jestClientFactory.getObject();

        // {}
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // bool
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // term
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "男");

        // filter
        boolQueryBuilder.filter(termQueryBuilder);

        // query
        searchSourceBuilder.query(boolQueryBuilder);

        // match
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "乒乓球");

        // must
        boolQueryBuilder.must(matchQueryBuilder);

        // terms
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByClass").field("class_id");

        // max
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");

        // aggs
        searchSourceBuilder.aggregation(termsAggregationBuilder.subAggregation(maxAggregationBuilder));

        // from size
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addType("_doc")
                .addIndex("student")
                .build();

        SearchResult result = jestClient.execute(search);

        // 1.total
        System.out.println("total:" + result.getTotal());
        // 2.hits
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:" + hit.index);
            System.out.println("_type:" + hit.type);
            System.out.println("_id:" + hit.id);
            System.out.println("_score:" + hit.score);
            Map source = hit.source;
            Set keySet = source.keySet();
            for (Object o : keySet) {
                System.out.println(o + ":" + source.get(o));
            }
        }

        // 3.aggs
        MetricAggregation resultAggregations = result.getAggregations();
        List<TermsAggregation.Entry> groupByClass = resultAggregations.getTermsAggregation("groupByClass").getBuckets();
        for (TermsAggregation.Entry entry : groupByClass) {
            System.out.println("key:" + entry.getKey());
            System.out.println("doc_count:" + entry.getCount());
            Long max = entry.getValueCountAggregation("groupByAge").getValueCount();
            System.out.println("value:" + max);
        }

        // 关闭连接
        jestClient.shutdownClient();
    }
}
