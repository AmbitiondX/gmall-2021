package com.atguigu.app;

import com.alibaba.fastjson.JSON;
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
        // 获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        // 获取连接
        while (true){
            canalConnector.connect();
            // 指定要监控的数据库
            canalConnector.subscribe("gmall20.*");
            // 获取message
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            // 判断entry小于等于零，线程睡眠，节约资源
            if (entries.size() <= 0){
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    // todo 获取表明
                    String tableName = entry.getHeader().getTableName();
                    // entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //判断entryType是否为ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        // 序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        // 反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        // todo 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // todo 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // todo 根据条件获取数据
                        handler(tableName,eventType,rowDatasList);
                    }


                }
            }

        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        // 获取订单表的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                // 获取存放列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                // 获取每个列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                System.out.println(jsonObject.toString());
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toString());
            }
        }
    }
}
