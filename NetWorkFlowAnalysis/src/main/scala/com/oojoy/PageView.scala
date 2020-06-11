package com.oojoy



import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time



//用户行为信息样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)
object PageView {
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
          .map(data=>("pv",1))
          .keyBy(_._1)
          .timeWindow(Time.hours(1))
          .sum(1)

    dataStream.print("page view")
    env.execute()
  }
}
