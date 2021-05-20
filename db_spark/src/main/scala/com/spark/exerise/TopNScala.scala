package com.spark.exerise

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TopN主播统计
 *
 * @author LR
 * @create 2021-05-20:11:57
 */
object TopNScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TopNScala")
      .setMaster("local")
    val sc = new SparkContext(conf)
    /*
    1：首先获取两份数据中的核心字段，使用fastjson包解析数据主播开播记录(video_info.log):主播ID：uid，直播间ID：vid，大区：area
    (vid,(uid,area)).用户送礼记录(gift_record.log)：直播间ID：vid，金币数量：gold(vid,gold)这样的话可以把这两份数据关联到一块
    就能获取到大区、主播id、金币这些信息了，使用直播间vid进行关联
    */
    val videoInfoRDD = sc.textFile("D:\\code\\JAVA\\bigdata\\video_info.log")
    val giftRecordRDD = sc.textFile("D:\\code\\JAVA\\bigdata\\gift_record.log")
    // 提取核心字段
    val videoInfoFieldRDD = videoInfoRDD.map(line=>{
      val jsonObj = JSON.parseObject(line)
      val vid = jsonObj.getString("vid")
      val uid = jsonObj.getString("uid")
      val area = jsonObj.getString("area")
      (vid, (uid,area))
    })

    val giftRecordFieldRDD = giftRecordRDD.map(line=>{
      val jsonObj = JSON.parseObject(line)
      val vid = jsonObj.getString("vid")
      val gold = jsonObj.getString("gold")
      (vid, gold)
    })

    //2：对用户送礼记录数据进行聚合，对相同vid的数据求和因为用户可能在一次直播中给主播送多次礼物(vid,gold_sum)
    val giftRecordFieldAggRDD = giftRecordFieldRDD.reduceByKey(_ + _)

    //3：把这两份数据join到一块，vid作为join的key(vid,((uid,area),gold_sum))
    val joinRDD = videoInfoFieldRDD.join(giftRecordFieldAggRDD)

    //4：使用map迭代join之后的数据，最后获取到uid、area、gold_sum字段由于一个主播一天可能会开播多次，后面需要基于uid和area再做一次聚合,
    // 所以把数据转换成这种格式uid和area是一一对应的，一个人只能属于大区((uid,area),gold_sum)
    val joinMapRDD = joinRDD.map(tup=>{
      //(vid,((uid,area),gold_sum))
      val uid = tup._2._1._1
      val area = tup._2._1._2
      val gold_sum = tup._2._2
      ((uid, area), gold_sum)
    })

    //5：使用reduceByKey算子对数据进行聚合((uid,area),gold_sum_all)
    val reduceRDD = joinMapRDD.reduceByKey(_ + _)

    //6：接下来对需要使用groupByKey对数据进行分组，所以先使用map进行转换因为我们要分区统计TopN，所以要根据大区分组
    //map：(area,(uid,gold_sum_all))
    //groupByKey: area,<(uid,gold_sum_all),(uid,gold_sum_all),(uid,gold_sum_all)>
    val groupRDD = reduceRDD.map(tup=>(tup._1._2, (tup._1._1, tup._2))).groupByKey()

    //7：使用map迭代每个分组内的数据，按照金币数量倒序排序，取前N个，最终输出area,topN这个topN其实就是把前几名主播的id还有金币数量拼接成一个字符串
    //(area,topN)
    val top3RDD = groupRDD.map(tup=>{
      val area = tup._1
      val top3 = tup._2.toList.sortBy(_._2).reverse.take(3).map(tup=>tup._1+":"+tup._2).mkString(",")
      (area, top3)
    })

    //8：使用foreach将结果打印到控制台，多个字段使用制表符分割   area	topN
    top3RDD.foreach(tup=>println(tup._1+"\t"+tup._2))

    sc.stop()
  }
}
