package com.oojoy.orderpay

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


//定义接收留样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMatchDetect {

  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

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

    val processedStream = orderEventStream.connect(receiptsEventStream)
      .process(new TxPayMatch())


    env.execute("tx match job")
  }

  class TxPayMatch() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
    //定义状态来保存支付事件和订单到账事件
    lazy val payedState:ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payed-state",classOf[OrderEvent]))
    lazy val receiptState :ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipts-state",classOf[ReceiptEvent]))

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    }
  }

}
