package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //1.创建Map集合用来存放返回的数据
        HashMap<String, Long> result = new HashMap<>();
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
//        System.out.println(list);
        for (Map map : list) {
            result.put((String)map.get("LH"),(Long)map.get("CT"));
        }
//        System.out.println(result);

        return result;
    }

    @Autowired
    private OrderMapper orderMapper;
    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
        // 获取dao层查询的数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        // 创建一个map封装list
        HashMap<String, Double> hourMap = new HashMap<>();

        for (Map map : list) {
            hourMap.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }

        return hourMap;
    }
}
