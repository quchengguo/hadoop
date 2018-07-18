package com.study.spark

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

/**
  * Created by quchengguo on 2018/7/18.
  *
  * 利用akka实现简易版的spark通信框架---master端
  */
class Master extends Actor {
  println("master constructor invoked")
  // 定义一个map集合，用于存放worker信息
  private val workerMap = new mutable.HashMap[String, WorkerInfo]
  // 定义一个list集合，用于存放workerInfo信息，方便之后按照资源排序
  private val workerList = new ListBuffer[WorkerInfo]
  // master定时检查时间间隔
  val CHECK_OUT_TIME_INTERVAL = 15000 //15秒

  override def preStart(): Unit = {
    println("preStart method invoked")
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_OUT_TIME_INTERVAL millis, self, CheckOutTime)
  }

  override def receive: Receive = {
    // master接受worker的注册信息
    case RegisterMessage(workerId, memory, cores) => {
      // 判断当前worker是否已经注册
      if (!workerMap.contains(workerId)) {
        // 保存信息到map集合中
        val workerInfo = new WorkerInfo(workerId, memory, cores)
        workerMap.put(workerId, workerInfo)
        // 保存workInfo信息到list集合中
        workerList += workerInfo

        // master反馈注册成功给worker
        sender ! RegisteredMessage(s"workerId:$workerId 注册成功")
      }
    }
    //master接受worker的心跳信息
    case SendHeartBeat(workerId) => {
      // 判断worker是否已经注册，master只接受已经注册过的worker的心跳信息
      if (workerMap.contains(workerId)) {
        // 获取workInfo信息
        val workerInfo = workerMap(workerId)
        // 获取系统当前时间
        val lastTime: Long = System.currentTimeMillis()
        workerInfo.lastHeartBeatTime = lastTime
      }
    }
    case CheckOutTime => {
      //过滤出超时的worker 判断逻辑： 获取当前系统时间 - worker上一次心跳时间 >master定时检查的时间间隔
      val outTimeWorkers: ListBuffer[WorkerInfo] = workerList.filter(x => System.currentTimeMillis() - x.lastHeartBeatTime > CHECK_OUT_TIME_INTERVAL)
      // 遍历超时的worker信息，然后移除超时的worker
      for (workerInfo <- outTimeWorkers) {
        for (workerInfo <- outTimeWorkers) {
          //获取workerid
          val workerId: String = workerInfo.workerId
          //从map集合中移除掉超时的worker信息
          workerMap.remove(workerId)
          //从list集合中移除掉超时的workerInfo信息
          workerList -= workerInfo

          println("超时的workerId:" + workerId)
        }
      }
      println("活着的worker总数：" + workerList.size)
      println(workerList.sortBy(x => x.memory).reverse.toList)

    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    //master的ip地址
    val host = args(0)
    //master的port端口
    val port = args(1)

    //准备配置文件信息
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    //配置config对象 利用ConfigFactory解析配置文件，获取配置信息
    val config = ConfigFactory.parseString(configStr)

    // 1、创建ActorSystem,它是整个进程中老大，它负责创建和监督actor，它是单例对象
    val masterActorSystem = ActorSystem("masterActorSystem", config)
    // 2、通过ActorSystem来创建master actor
    val masterActor: ActorRef = masterActorSystem.actorOf(Props(new Master), "masterActor")
  }
}
