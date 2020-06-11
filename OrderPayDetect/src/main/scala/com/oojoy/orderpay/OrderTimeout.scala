package com.oojoy.orderpay




import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
//定义输入的样例类
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime:Long)
//定义输出的样例类
case class OrderResult(orderId: Long, eventResult: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream: DataStream[OrderEvent] = env.readTextFile("D:\\IDEASpace\\flink-userBehavior\\OrderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val datastr: Array[String] = data.split(",")
        OrderEvent(datastr(0).trim.toLong, datastr(1).trim, datastr(2).trim, datastr(3).trim.toLong)
      })
      .assignAscendingTimestamps(s => s.eventTime + 1000L)
      .keyBy(_.orderId)

    //定义一个匹配时间窗口的模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(1))

    //把模式应用到stream上，得到patternStream
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    //定义一个输出标签,
    val orderTimeoutOutPut: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")
    //

    val resultStream = patternStream.select(orderTimeoutOutPut,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    )

    resultStream.print()
    resultStream.getSideOutput(orderTimeoutOutPut).print()

    env.execute()
  }

}

//自定义超时时间处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId= map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId,"timeout")
  }
}


class OrderPaySelect extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val parOrderId = map.get("follow").iterator().next().orderId
    OrderResult(parOrderId,"payed success")
  }
}*/
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 *
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.orderpay_detect
 * Version: 1.0
 *
 * Created by wushengran on 2019/9/25 9:17
 */

// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取订单数据
    val orderEventStream: DataStream[OrderEvent] = env.readTextFile("D:\\IDEASpace\\flink-userBehavior\\OrderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 2. 定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3. 把模式应用到stream上，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 4. 调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

// 自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
