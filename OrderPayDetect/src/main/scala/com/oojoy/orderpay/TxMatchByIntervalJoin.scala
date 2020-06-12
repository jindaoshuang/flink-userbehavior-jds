package com.oojoy.orderpay


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchByIntervalJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取订单数据
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile("D:\\IDEASpace\\flink-userBehavior\\OrderPayDetect\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //2.读取收据流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptsEventStream = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val receDataArr: Array[String] = data.split(",")
        ReceiptEvent(receDataArr(0).trim, receDataArr(1).trim, receDataArr(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val processedStream = orderEventStream.intervalJoin(receiptsEventStream)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print()

    env.execute()

  }

  class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      out.collect((left,right))
    }
  }

}
