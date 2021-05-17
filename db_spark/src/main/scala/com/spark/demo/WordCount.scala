package com.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author LR
 * @create 2021-05-17:18:59
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf()
    conf.setAppName("WordCount") // 设置本地名称
      //.setMaster("local") // local 表示在本地执行

    val sc = new SparkContext(conf)

    // 加载数据
    var path = "D:\\BaiduNetdiskDownload\\大数据\\git-master\\bigdata_course_materials\\spark\\上\\hello.txt"
    if (args.length == 1){
      path = args(0)
    }
    val linesRDD = sc.textFile(path)

    // 对数据进行分割
    val wordsRDD = linesRDD.flatMap(_.split(" "))

    // 迭代word
    val pairRDD = wordsRDD.map((_,1))

    // 根据key进行分类聚合
    val wordCountRDD = pairRDD.reduceByKey(_ + _)

    // 将结果打印到控制台
    wordCountRDD.foreach(WordCount =>  println(WordCount._1 + "--" + WordCount._2))

    // 停止SparkContext
    sc.stop()

  }


}
