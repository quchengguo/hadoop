package com.study1

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by quchengguo on 2018/7/22.
  * 通过spark实现ip地址的查询
  * 思路：
  * 1、加载城市ip段信息，获取ip起始数字和结束数字，经度，维度
  * 2、加载日志数据，获取ip信息，然后转换为数字，和ip段比较
  * 3、比较的时候采用二分法查找，找到对应的经度和维度
  * 4、然后对经度和维度做单词计数
  */
object IPLocaltion {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("IPLocaltion_Test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    // 1.获取基站数据
    val data: RDD[(String)] = sc.textFile("E:\\ip.txt")

    // 2.对数据进行切分，获取需要的字段（ipStart, ipEnd, 城市位置， 经度， 纬度）
    val jizhanRDD: RDD[(String, String, String, String, String)] = data.map(_.split("\\|")).map(x => (x(2), x(3), x(4) + "-" + x(5) + "-" + x(6) + "-" + x(7) + "-" + x(8), x(13), x(14)))
    val jizhanData: Array[(String, String, String, String, String)] = jizhanRDD.collect()
    // 广播变量，一个只读数据区，所有task都能读到的地方
    val jizhanBroadcast: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(jizhanData)
    // 读取目标数据
    val destData: RDD[(String)] = sc.textFile("E:\\20090121000132.394251.http.format")
    // 获取ip地址字段
    val ipData: RDD[(String)] = destData.map(_.split("\\|")).map(x => x(1))
    // 把IP地址转换为long类型，然后通过二分法去基站数据中查找，找到的纬度做wordCount
    val result = ipData.mapPartitions(iter => {
      //获取广播变量中的值
      val valueArr: Array[(String, String, String, String, String)] = jizhanBroadcast.value

      //todo:操作分区中的itertator
      iter.map(ip => {
        //将ip转化为数字long
        val ipNum: Long = ipToLong(ip)

        //拿这个数字long去基站数据中通过二分法查找，返回ip在valueArr中的下标
        val index: Int = binarySearch(ipNum, valueArr)

        //根据下标获取对一个的经纬度
        val tuple = valueArr(index)
        //返回结果 ((经度，维度)，1)
        ((tuple._4, tuple._5), 1)

      })
    })
    // 相同纬度和经度出现的次数进行累加
    val resultFinal: RDD[((String, String), Int)] = result.reduceByKey(_ + _)
    resultFinal.foreach(println)

    // 将结果保存在mysql中
    resultFinal.map(x=>(x._1._1,x._1._2,x._2)).foreachPartition(data2Mysql)
    sc.stop()
  }

  // IP转换为long类型
  def ipToLong(ip: String): Long = {
    val ipArray: Array[String] = ip.split("\\.")
    var ipNum = 0L

    for (i <- ipArray) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  //通过二分查找法,获取ip在广播变量中的下标
  def binarySearch(ipNum: Long, valueArr: Array[(String, String, String, String, String)]): Int = {

    //开始下标
    var start = 0
    //结束下标
    var end = valueArr.length - 1

    while (start <= end) {
      val middle = (start + end) / 2
      if (ipNum >= valueArr(middle)._1.toLong && ipNum <= valueArr(middle)._2.toLong) {
        return middle
      }
      if (ipNum > valueArr(middle)._2.toLong) {
        start = middle
      }

      if (ipNum < valueArr(middle)._1.toLong) {
        end = middle
      }
    }
    -1
  }

  // 保存数据到mysql中
  def data2Mysql(iterator: Iterator[(String, String, Int)]): Unit = {
    // 1.创建数据库连接
    var conn: Connection = null
    // 2.创建PreparedStatement对象
    var ps: PreparedStatement = null
    // 3.采用占位符？的方式写sql语句
    var sql = "insert into iplocaltion(longitude, latitude, total_count) values (?, ?, ?)"
    // 4.获取数据库连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
    // 5.真正操作数据库的时候添加try...catch
    try {
      iterator.foreach(line => {

        //todo:预编译sql语句
        ps = conn.prepareStatement(sql)

        //todo:对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
        ps.setString(1, line._1)
        ps.setString(2, line._2)
        ps.setLong(3, line._3)
        ps.execute()
      })
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}
