package com.spark.demo

import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

/**
 * @author LR
 * @create 2021-05-19:12:17
 */
object CreateRDDByArray {

  def main(args: Array[String]): Unit = {

    // 创建SparkContext
    val conf = new SparkConf()
    conf.setAppName("CreateRDDByArray")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //创建集合
    val arr = Array(1,2,3,4,5,6)
    // 基于集合创建RDD
    val rdd = sc.parallelize(arr)
    // 对集合中数据进行求和
    val sum = rdd.reduce(_ + _)
    println(sum)
  }

}
