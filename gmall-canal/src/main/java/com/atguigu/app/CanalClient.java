package com.atguigu.app;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        // 获取canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        while (true) {
            // 保持连接
            canalConnector.connect();
            canalConnector.subscribe("gmall.*");
            // 设置message批次大小
            Message message = canalConnector.get(100);
            // 获得message中的entry集
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.isEmpty()){
                System.out.println("暂时还没有数据，睡5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            for (CanalEntry.Entry entry : entries) {
                // 获得entry中表名
                String tableName = entry.getHeader().getTableName();
                // 获得entry的entrytype
                CanalEntry.EntryType entryType = entry.getEntryType();
                if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                    // 获得序列化内容
                    ByteString storeValue = entry.getStoreValue();
                    // 反序列化 解析
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                    // 将order_info表中新增的数据进一步处理
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
                        handler(rowChange,GmallConstants.KAFKA_TOPIC_ORDER);
                    }else if ("order_detail".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
                        handler(rowChange,GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
                    }else if ("user_info".equals(tableName)&&(CanalEntry.EventType.INSERT.equals(eventType)||CanalEntry.EventType.UPDATE.equals(eventType))){
                        handler(rowChange, GmallConstants.KAFKA_TOPIC_USER);
                    }


                }

            }
        }
    }

    private static void handler(CanalEntry.RowChange rowChange, String kafkaTopic ) {
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            // 获取存放每个列的集合
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(),column.getValue());
            }
            MyKafkaSender.send(kafkaTopic,jsonObject.toString());
        }
    }
}
