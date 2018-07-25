package com.study1.sparkstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by quchengguo on 2018/7/25.
  * 利用SparkStreaming的开窗函数reduceByKeyAndWindow实现单词计数
  *
  */
object SparkStreamingSocketWindow {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketWindow").setMaster("local[2]")
    // 2.创建sparkcontext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    // 3.创建streamingcontext
    val ssc = new StreamingContext(sc, Seconds(5))
    // 设置checkpoint目录，如果不记录历史数据的话checkpoint没有用
    ssc.checkpoint("./ck")
    // 4.接收socket数据
    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    // 5.切分每一行
    val words: DStream[String] = stream.flatMap(_.split(" "))
    // 6.把每一个单词记为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    // 7.相同单词出现的次数累加
    //reduceByKeyAndWindow该方法需要三个参数
    //reduceFunc：需要一个函数
    //windowDuration:表示窗口的长度
    //slideDuration:表示窗口滑动时间间隔，即每隔多久计算一次
    val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(10))


    // 8.打印结果
    result.print()
    // 9.开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
