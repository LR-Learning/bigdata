package com.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author LR
 * @create 2021-05-19:12:44
 */
object TransformationOpScala {



  def main(args: Array[String]): Unit = {
    var sc = getSparkContext
    // 将每个数值乘以2
//    mapOp(sc)
    // 过滤出集合中的偶数
    filterOp(sc)
  }

  def mapOp(sc: SparkContext): Unit = {
    val dataRdd = sc.parallelize(Array(1,2,3,4,5,6))

    dataRdd.map(_ * 2).foreach(println(_))

  }

  def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("TransformationOpScala")
      .setMaster("local")

    new SparkContext(conf)
  }

  def filterOp(sc: SparkContext): Unit = {
    val dataRdd = sc.parallelize(Array(1,2,3,4,5,6))
    dataRdd.filter(_ % 2 == 0).foreach(println(_))

  }

}
