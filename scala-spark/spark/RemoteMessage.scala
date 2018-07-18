package com.study.spark

/**
  * Created by quchengguo on 2018/7/18.
  */
trait RemoteMessage extends Serializable {

}

// 向master发送注册信息封装成样例类
case class RegisterMessage(val workerId: String, val memory: Int, val cores: Int) extends RemoteMessage

// master反馈注册成功信息给worker，由于不在同一进程中，也需要实现序列化
case class RegisteredMessage(message: String) extends RemoteMessage

// worker向worker发送心跳，由于在同一进程中，不需要实现序列化
case object HeartBeat

// worker 向master发送心跳，需要实现序列化
case class SendHeartBeat(val workerId: String) extends RemoteMessage

// master自己向自己发送消息，由于在同一进程中，不需要实现序列化
case object CheckOutTime