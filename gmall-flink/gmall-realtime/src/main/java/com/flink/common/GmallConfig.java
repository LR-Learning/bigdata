package com.flink.common;

/**
 * @author LR
 * @create 2022-06-19:15:32
 */
public class GmallConfig {

    //Phoenix库名
    public static final String HASE_SCHEMA = "GMALL_REALTIME";
    // Phoenix驱动
    public static final String HASE_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix 连接参数
    public static final String HASE_SERVER = "jdbc:phoenix:bigdata01,bigdata02,bigdata03:2181";

}
