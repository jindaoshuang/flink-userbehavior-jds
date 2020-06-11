package com.oojoy.com


import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//输入事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

//输出的异常信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warnMessage: String)

object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //1、读取事件数据

    val logeventStream = env.readTextFile("D:\\IDEASpace\\flink-userBehavior\\loginFailDetect\\src\\main\\resources\\LoginLog.csv")
      .map(data => {
        val datastr = data.split(",")
        LoginEvent(datastr(0).trim.toLong, datastr(1).trim, datastr(2).trim, datastr(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      }).keyBy(_.userId)

    //2、定义匹配模式
    val loginFaliPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    //3、在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(logeventStream, loginFaliPattern)

    //4、 从pattern stream 上应用 select function 检出匹配时间序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print("结果")
    env.execute()
  }
}


class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, firstFail.userId + "连续登陆失败两次")
  }
}