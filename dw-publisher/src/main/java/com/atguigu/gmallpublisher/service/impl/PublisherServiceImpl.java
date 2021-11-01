package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
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
}
