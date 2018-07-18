package com.study.spark

/**
  * Created by quchengguo on 2018/7/18.
  * 分装worker信息
  */
class WorkerInfo(val workerId:String,val memory:Int,val cores:Int) {
  var lastHeartBeatTime:Long =_

  override def toString: String = {
    s"workerId:$workerId , memory:$memory , cores:$cores"
  }
}
