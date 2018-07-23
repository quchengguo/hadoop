package com.study1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by quchengguo on 2018/7/22.
  * 统计PV
  */
object PV {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    val context = new SparkContext(sparkConf)
    // 1.读取数据
    val file : RDD[String]= context.textFile("E:\\access.log")
    // 2.计算总数
    println(file.count())
    context.stop()
  }
}
