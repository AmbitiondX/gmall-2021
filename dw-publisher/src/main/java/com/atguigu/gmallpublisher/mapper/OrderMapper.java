package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    // 查询当日交易额总数
    public Double selectOrderAmountTotal(String date);
    // 查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);
}
