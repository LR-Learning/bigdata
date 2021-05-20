package com.spark.exercise;

import avro.shaded.com.google.common.collect.Lists;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * @author LR
 * @create 2021-05-20:12:44
 */
public class TopNJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("TopNJava")
         .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> videoInfoRDD = sc.textFile("D:\\code\\JAVA\\bigdata\\video_info.log");
        JavaRDD giftRecordRDD = sc.textFile("D:\\code\\JAVA\\bigdata\\gift_record.log");
        JavaPairRDD<String, Tuple2<String,String>> videoInfoFieldRDD = videoInfoRDD.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
           public Tuple2<String, Tuple2<String, String>> call(String line) throws Exception {
               JSONObject jsonObject = JSON.parseObject(line);
               String vid = jsonObject.getString("vid");
               String uid = jsonObject.getString("uid");
               String area = jsonObject.getString("area");

               return new Tuple2<String, Tuple2<String, String>>(vid, new Tuple2<String, String>(uid, area));
           }
        });

        JavaPairRDD<String, Integer> giftRecordFieldRDD = giftRecordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                JSONObject jsonObject = JSON.parseObject(line);
                String vid = jsonObject.getString("vid");
                Integer gold = jsonObject.getInteger("gold");
                return new Tuple2<String, Integer>(vid, gold);
            }
        });

        JavaPairRDD<String, Integer> giftRecordFieldAggRDD = giftRecordFieldRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        JavaPairRDD<Tuple2<String, String>, Integer> joinMapRDD = videoInfoFieldRDD.join(giftRecordFieldAggRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Integer>>, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, Tuple2<Tuple2<String, String>, Integer>> tup) throws Exception {

               String uid = tup._2._1._1;
               String area = tup._2._1._2;
               Integer gold_sum = tup._2._2;

               return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(uid, area), gold_sum);
            }
        });

        JavaPairRDD<Tuple2<String, String>, Integer> reduceRDD = joinMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tup) throws Exception {
                return new Tuple2<String, Tuple2<String, Integer>>(tup._1._2, new Tuple2<String, Integer>(tup._1._1, tup._2));
            }
        }).groupByKey();

        JavaRDD<Tuple2<String, String>> top3 = groupRDD.map(new Function<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> tup) throws Exception {
                String area = tup._1;
                ArrayList<Tuple2<String, Integer>> tupleList = Lists.newArrayList(tup._2);

                Collections.sort(tupleList, new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
                        return t2._2 - t1._2;
                    }
                });

                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < tupleList.size(); i++) {
                    if (i < 3){
                        Tuple2<String, Integer> t = tupleList.get(i);
                        if (i != 0){
                            sb.append(",");
                        }
                        sb.append(t._1 + ":" + t._2);
                    }
                }
                return new Tuple2<String, String>(area, sb.toString());
            }
        });

        top3.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tup) throws Exception {
                System.out.println(tup._1 + "\t" + tup._2);
            }
        });

        sc.stop();
    }
}
