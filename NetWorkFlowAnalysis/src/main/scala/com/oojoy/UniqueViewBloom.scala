package com.oojoy

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


object UinqueViewBloom {
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
      .filter(_.behavior == "pv") //只统计PV操作
      .map(data => ("dumpKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //每来一条数据触发一次窗口操作，因为单个窗口放不下所有的数据
      .process(new UvCountWithBloom())

    dataStream.print("uv with bloom job")
    env.execute()

  }


  //自定义窗口触发器
  class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE //每来一条数据就直接触发窗口操作，并清除窗口的状态
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }


}


class Bloom(size: Long) extends Serializable {
  //位图的总大小
  private val cap = if (size > 0) size else 1 << 27

  //定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }

    result & (cap - 1)
  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] with Serializable {
  // 创建 redis 连接
  lazy val jedis = new Jedis("node03", 6379)
  jedis.auth("123456")
  lazy val bloom = new Bloom(1 << 29)
  override def process(key: String, context: Context, elements: Iterable[(String,

  Long)], out: Collector[UvCount]): Unit = {
    val storeKey = context.window.getEnd.toString
    var count = 0L
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}