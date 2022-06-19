package com.flink.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.bean.TableProcess;
import com.flink.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author LR
 * @create 2022-06-19:15:27
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.HASE_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.HASE_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // 主流
        // 1.获取状态数据

        // 2.过滤字段

        // 3.分流
    }

    // value:{"db":"","tn":"","before":"","after":"","type":""}
    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        // 广播流
        // 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        // 2.建表
        if (TableProcess.SINK_TYPE_HASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPK(),
                    tableProcess.getSinkExtend());
        }

        // 3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    // 建表语句: create table if not exists db.tn(id varchar primary key, tm_name varchar) xxxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPK, String sinkExtend) {
        if (sinkPK == null){
            sinkPK = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }

        StringBuffer createTableSQL = new StringBuffer("create table if not exists")
                .append(GmallConfig.HASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {

            String field = fields[i];
            // 判断是否为主键
            if (sinkPK.equals(field)){
                createTableSQL.append(" varchar primary key");
            }else {
                createTableSQL.append(" varchar");
            }

            // 判断是否为最后一个字段,如果不是,则添加","
            if (i < fields.length-1){
                createTableSQL.append(",");
            }

            createTableSQL.append(")").append(sinkExtend);

            System.out.println(createTableSQL);
        }
    }
}
