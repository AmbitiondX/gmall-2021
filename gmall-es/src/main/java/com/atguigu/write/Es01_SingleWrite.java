package com.atguigu.write;

import com.atguigu.bean.Music;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;


import java.io.IOException;

public class Es01_SingleWrite {
    public static void main(String[] args) {
        // 创建es客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();
        // 设置工厂连接地址
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(clientConfig);
        // 创建es客户端
        JestClient jestClient = jestClientFactory.getObject();

        // 创建数据新行动容器index
//        Index index = new Index.Builder("{\n" +
//                "  \"id\":\"001\",\n" +
//                "  \"name\":\"my hert will go on\"\n" +
//                "}")
//                .id("1001")
//                .type("_doc")
//                .index("music_0625")
//                .build();

        // 使用javaBean创建index
        Music sugar = new Music(101, "Sugar");
        Index index = new Index.Builder(sugar)
                .id("1002")
                .type("_doc")
                .index("music_0625")
                .build();

        try {
            // 使用客户端执行行动容器
            jestClient.execute(index);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭jestClient连接，使用close()有可能关不掉，所以使用原来的方法
        jestClient.shutdownClient();
    }
}
