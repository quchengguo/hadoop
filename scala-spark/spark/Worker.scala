package com.study.spark

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

/**
  * Created by quchengguo on 2018/7/18.
  *
  * 利用akka实现简易版的spark通信框架---worker端
  */
class Worker(val memory: Int, val cores: Int, val masterHost: String, val masterPort: String) extends Actor {
  println("worker constructor invoked")
  // 定义workId
  private val workerId: String = UUID.randomUUID().toString
  //定义发送心跳的时间间隔
  val SEND_HEART_HEAT_INTERVAL = 10000 //10秒
  //定义全局变量
  var master: ActorSelection = _

  override def preStart(): Unit = {
    println("preStart method invoked")
    // 获取master actor 的引用
    master = context.actorSelection(s"akka.tcp://masterActorSystem@$masterHost:$masterPort/user/masterActor")
    master ! RegisterMessage(workerId, memory, cores)
  }

  override def receive: Receive = {
    //worker接受master的反馈信息
    case RegisteredMessage(message) => {
      println(message)

      //向master定期的发送心跳
      //worker先自己给自己发送心跳
      //需要手动导入隐式转换
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, SEND_HEART_HEAT_INTERVAL millis, self, HeartBeat)
    }
    //worker发送心跳
    case HeartBeat => {
      //这个时候才是真正向master发送心跳
      master ! SendHeartBeat(workerId)
    }
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    //定义worker的IP地址
    val host = args(0)
    //定义worker的端口
    val port = args(1)

    //定义worker的内存
    val memory = args(2).toInt
    //定义worker的核数
    val cores = args(3).toInt
    //定义master的ip地址
    val masterHost = args(4)
    //定义master的端口
    val masterPort = args(5)

    //准备配置文件
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    //通过configFactory来解析配置信息
    val config = ConfigFactory.parseString(configStr)
    // 1、创建ActorSystem，它是整个进程中的老大，它负责创建和监督actor
    val workerActorSystem = ActorSystem("workerActorSystem", config)
    // 2、通过actorSystem来创建 worker actor
    val workerActor: ActorRef = workerActorSystem.actorOf(Props(new Worker(memory, cores, masterHost, masterPort)), "workerActor")

    //向worker actor发送消息
    workerActor ! "connect"
  }
}