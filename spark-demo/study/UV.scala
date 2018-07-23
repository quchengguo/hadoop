package com.study1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by quchengguo on 2018/7/22.
  * 统计UV量
  */
object UV {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    // 1.读取数据
    val file: RDD[String] = sc.textFile("E:\\access.log")

    // 2.对每一行分割，获取IP地址
    val ips: RDD[(String)] = file.map(_.split(" ")).map(x => x(0))

    // 3.对ip地址进行去重，最后输出格式（“UV”， 1）
    val uvAndOne: RDD[(String, Int)] = ips.distinct().map(x => ("UV", 1))

    // 4.聚合输出
    val totalUV: RDD[(String, Int)] = uvAndOne.reduceByKey(_ + _)
    totalUV.foreach(println)

    // 5.数据结果保存
    totalUV.saveAsTextFile("E:\\result")

    sc.stop()

  }
}
