package com.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;


import java.util.Arrays;
import java.util.List;

/**
 * @author LR
 * @create 2021-05-19:12:25
 */
public class CreatRDDByArrayJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount")
            .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> arr = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> rdd = sc.parallelize(arr);
        Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        System.out.println(sum);
    }
}
