package com.bigdata.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author LR
 * @create 2021-05-12:16:09
 *
 * JDBC操作hive
 * 注意：需要先启动hiverserver2 服务
 */
public class HiveJdbcDemo {
    public static void main(String[] args) throws Exception{
        String jdbcUrl = "jdbc:hive2://192.168.142.103:10000";
        // 获取链接
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "any");

        // 获取Statement
        Statement stmt = conn.createStatement();

        // 指定查询sql
        String sql = "select * from t1";
        // 执行SQL
        ResultSet res = stmt.executeQuery(sql);
        // 读取结果
        while (res.next()){
            System.out.println(res.getInt("id") + "\t" + res.getString("name"));
        }

    }
}
