package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date){
        // 获取当天日活
        int dauTotal = publisherService.getDauTotal(date);

        // 创建存放新增日活的map集合，并存放数据
        HashMap<String, Object> dauMap = new HashMap<>();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        // 创建存放新增设备的map集合,并存放数据
        HashMap<String, Object> devMap = new HashMap<>();

        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value","233");

        // 创建存放交易额总数的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", orderAmountTotal);

        // 创建一个存放map的list
        ArrayList<HashMap> result = new ArrayList<>();

        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        // 使用fastjson.JSON将list集合处理为字符串
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date){
        // 获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        //1.获取Service层处理过后的数据
        if ("dau".equals(id)){
            todayMap = publisherService.getDauTotalHourMap(date);
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        } else if ("order_amount".equals(id)){
            todayMap = publisherService.getOrderAmountHourMap(date);
            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
        }


        //2.创建存放结果数据的Map集合
        HashMap<String, Map> result = new HashMap<>();
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        return JSONObject.toJSONString(result);

    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(
            @RequestParam("date") String date,
            @RequestParam("startpage") Integer startpage,
            @RequestParam("size") Integer size,
            @RequestParam("keyword") String keyword
    ) throws IOException {

        Map result = publisherService.getSaleDetail(date, startpage, size, keyword);

        return JSONObject.toJSONString(result);
    }
}
