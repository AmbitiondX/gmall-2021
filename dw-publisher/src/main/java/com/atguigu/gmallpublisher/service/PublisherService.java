package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    //日活总数数据接口
    public Integer getDauTotal(String date);
    //日活分时数据接口
    public Map getDauTotalHourMap(String date);

    //交易额总数
    public Double getOrderAmountTotal(String date);
    //交易额分时数据
    public Map<String, Double> getOrderAmountHourMap(String date);

    //灵活查询数据接口
    public Map getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException;



}
