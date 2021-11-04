package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Es03_JsonRead {
    public static void main(String[] args) throws IOException {
        // 1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        // 2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // 3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        // 4.读数据
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"男\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"乒乓球\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupByclass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\"\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"groupByAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 2\n" +
                "}").build();

        SearchResult result = jestClient.execute(search);

        // a.获取命中条数
        System.out.println("total:" + result.getTotal());

        // b.获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:" + hit.index);
            System.out.println("_type:" + hit.type);
            System.out.println("_id:" + hit.id);
            Map source = hit.source;
            Set set = source.keySet();
            for (Object o : set) {
                System.out.println(o + ":" + source.get(o));
            }
        }

        // c.获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();

        // c.2 班级聚合数据
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByclass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("keys:" + bucket.getKey());
            System.out.println("doc_count:" + bucket.getCount());

            MaxAggregation maxAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:" + maxAge.getMax());
        }

        // 关闭连接
        jestClient.shutdownClient();
    }
}
