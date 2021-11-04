package com.atguigu.write;

import com.atguigu.bean.Music;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es02_BulkWrite {
    public static void main(String[] args) throws IOException {
        // 创建es客户端工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        // 创建客户端配置文件
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        // 加载配置文件
        jestClientFactory.setHttpClientConfig(build);
        // 创建es客户端
        JestClient jestClient = jestClientFactory.getObject();

        Index index102 = new Index.Builder(new Music(102, "abc")).build();
        Index index103 = new Index.Builder(new Music(103, "def")).build();
        Index index104 = new Index.Builder(new Music(104, "ghi")).build();
        Index index105 = new Index.Builder(new Music(105, "jkl")).build();

        Bulk bulk = new Bulk.Builder()
                .defaultType("_doc")
                .defaultIndex("music_0625")
                .addAction(index102)
                .addAction(index103)
                .addAction(index104)
                .addAction(index105)
                .build();
        jestClient.execute(bulk);


        // 关闭客户端
        jestClient.shutdownClient();
    }
}
