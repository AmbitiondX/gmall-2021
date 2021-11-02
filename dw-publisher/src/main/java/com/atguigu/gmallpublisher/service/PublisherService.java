package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    public Integer getDauTotal(String date);
    public Map getDauTotalHourMap(String date);

    //交易额总数
    public Double getOrderAmountTotal(String date);
    //交易额分时数据
    public Map<String, Double> getOrderAmountHourMap(String date);

}
