package com.study1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by quchengguo on 2018/7/22.
  * 访问前N
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    // 1.读取数据
    val file: RDD[String] = sc.textFile("E:\\chuanzhi\\学习资料\\Hadoop\\spark\\day02\\资料\\运营商日志\\access.log")

    // 2.将一行数据作为输入,输出（来源URL, 1）
    val refUrlAndOne: RDD[(String, Int)] = file.map(_.split(" ")).filter(_.length > 10).filter(x=>x(10)!="\"-\"").map(x => (x(10), 1))

    // 3.聚合  降序排序
    val result : RDD[(String, Int)]= refUrlAndOne.reduceByKey(_+_).sortBy(_._2, false)

    // 4.通过take取TopN，这里取前5
    val finalResult :Array[(String, Int)]= result.take(5)

    println(finalResult.toBuffer)
    sc.stop()
  }
}
