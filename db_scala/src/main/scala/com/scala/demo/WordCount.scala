package com.scala.demo

import scala.io.Source.fromFile

/**
 * 统计字符
 *
 * @author LR
 * @create 2021-05-16:14:58
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val lines1 = fromFile("D:\\BaiduNetdiskDownload\\大数据\\git-master\\bigdata_course_materials\\scala\\a.txt").mkString
    val lines2 = fromFile("D:\\BaiduNetdiskDownload\\大数据\\git-master\\bigdata_course_materials\\scala\\b.txt").mkString
    // 将文件拼在一块
    val lines = List(lines1, lines2)
    // 切割单词
    val count = lines.flatMap(_.split(" ")).map((_,1)).map(tup=>tup._2).reduceLeft(_+_)
    println(count)
  }


}
