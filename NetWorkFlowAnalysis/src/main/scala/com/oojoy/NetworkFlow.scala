package com.oojoy


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

//定义日志输入样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//窗口聚合函数样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = env.readTextFile("D:\\IDEASpace\\flink-userBehavior\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")

    val resultStream: DataStream[String] = inputStream.map(data => {
      val dataArray = data.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val time: Long = format.parse(dataArray(3)).getTime
      ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, time, dataArray(5).trim, dataArray(6).trim)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNURL(5))

    resultStream.print("urlResutl")

    env.execute()
  }

}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNURL(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val UrlListBuffer = new ListBuffer[UrlViewCount]()
    val value: util.Iterator[UrlViewCount] = urlState.get().iterator()
    while (value.hasNext) {
      UrlListBuffer += value.next()
    }
    urlState.clear()
    val sortedUrls: ListBuffer[UrlViewCount] = UrlListBuffer.sortWith(_.count > _.count).take(topSize)


    //格式化结果输出
    val result = new StringBuilder

    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedUrls.indices) {
      val currentUrl: UrlViewCount = sortedUrls(i)
      result.append("NO").append(i + 1).append(":").append(" URL=").append(currentUrl.url)
        .append(" count=").append(currentUrl.count).append("\n")
    }

    result.append("++++++++++++++++++++++++++++++++++++")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}