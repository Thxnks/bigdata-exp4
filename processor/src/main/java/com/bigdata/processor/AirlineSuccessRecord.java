package com.bigdata.processor;

import java.io.Serializable;

// Spark 聚合使用的 success 明细记录。
public class AirlineSuccessRecord implements Serializable {
    // 小时粒度统计时间，例如 2018-08-30 19。
    private String statHour;
    // 航空公司代码，例如 CA。
    private String airlineCode;

    // Spark Bean 编码需要无参构造方法。
    public AirlineSuccessRecord() {
    }

    // 创建一条 success 明细记录。
    public AirlineSuccessRecord(String statHour, String airlineCode) {
        this.statHour = statHour;
        this.airlineCode = airlineCode;
    }

    // 返回小时粒度统计时间。
    public String getStatHour() {
        return statHour;
    }

    // 设置小时粒度统计时间。
    public void setStatHour(String statHour) {
        this.statHour = statHour;
    }

    // 返回航空公司代码。
    public String getAirlineCode() {
        return airlineCode;
    }

    // 设置航空公司代码。
    public void setAirlineCode(String airlineCode) {
        this.airlineCode = airlineCode;
    }
}
