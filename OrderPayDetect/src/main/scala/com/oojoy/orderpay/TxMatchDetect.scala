package com.oojoy.orderpay

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


//定义接收留样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)


/**
 * 两条流对账测试
 * 两条流按照txid做分组，保证同一交易进入同一个流中做比对
 * 正常情况主流输出，比对失败测流输出
 */
object TxMatchDetect {

  //定义侧输出流
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

    processedStream.print("matched ")
    processedStream.getSideOutput(unmatchedPays).print("pay unmatched")
    processedStream.getSideOutput(unmatchReceipts).print("receipt unmatched")

    env.execute("tx match job")
  }

  //todo 因为两条流都按照txid做了分组，只要是能合并到一起的流都是同一个txid
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    //定义状态来保存支付事件和订单到账事件
    lazy val payedState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payed-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipts-state", classOf[ReceiptEvent]))

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      //判断是否有对应的到账事件
      val receipt = receiptState.value()
      if (receipt != null) {
        //有对应的到账事件，在主流输出
        out.collect((pay, receipt))
        receiptState.clear()
      } else {
        //没有对应的到账事件
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000l + 5000L)
        payedState.update(pay)
      }

    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      val pay = payedState.value()
      if (pay != null) {
        out.collect((pay, receipt))
        payedState.clear()
      } else {
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000l)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit ={
      //其中一条来了，说明另一条咩来
      if(payedState.value() != null){
        //receipt 没有来  把pay输出到测输出流中
        ctx.output(unmatchedPays,payedState.value())
      }
      if(receiptState.value() != null){
        ctx.output(unmatchReceipts,receiptState.value())
      }
      payedState.clear()
      receiptState.clear()
    }
  }

}
