package com.flink.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.flink.bean.TableProcess;
import com.flink.function.CustomerDeserialization;
import com.flink.function.TableProcessFunction;
import com.flink.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.sound.sampled.Port;


/**
 * @author LR
 * @create 2022-06-19:14:03
 */
public class BaseDBApp {

    public static void main(String[] args) {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";

        DataStreamSource<String> kafkaDS= env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3.将每行数据转换为JSON对象并过滤(delete)       主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // 取出数据的操作类型
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });

        // 4.使用FlinkCDC消费配置表并处理成广播流         广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("bigdata01")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall-realtime")
                .tableList("table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state", String.class,TableProcess.class );
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);


        // 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        // 6.处理数据  广播流数据，主流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag"){};
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        // 7. 提取kafka流数据和Hase流数据
        DataStream<JSONObject> hbse = kafka.getSideOutput(hbaseTag);

        // 8.将kafka数据写入kafka主题，将Hase数据写入Phoenix表

        // 9.启动任务

    }
}
