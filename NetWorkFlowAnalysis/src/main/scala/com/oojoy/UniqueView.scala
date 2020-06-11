package com.oojoy

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(windowEnd:Long,unCount:Long)
object UniqueView {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = env.readTextFile("D:\\IDEASpace\\flink-userBehavior\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArr = data.split(",")
        UserBehavior(dataArr(0).trim.toLong, dataArr(1).trim.toLong, dataArr(2).trim.toLong, dataArr(3).trim, dataArr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply( new UV())

    dataStream.print()
    env.execute()
  }

}

class UV extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var userIdSet = Set[Long]()
    for(userBehavior <- input){
      userIdSet += userBehavior.userId
    }

    out.collect(UvCount(window.getEnd,userIdSet.size))
  }
}