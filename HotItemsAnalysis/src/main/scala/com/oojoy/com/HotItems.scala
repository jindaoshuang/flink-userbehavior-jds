package com.oojoy.com


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//用户行为信息样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)

//定义窗口聚合样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
 * 每5分钟计算当前一小时内的品类top3
 */
object HotItems {
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.186.100:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val dataStream = env.readTextFile("D:\\IDEASpace\\flink-userBehavior\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      val dataStream=env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(),
          properties))
      .map(data => {
        val dataArr = data.split(",")
        UserBehavior(dataArr(0).trim.toLong, dataArr(1).trim.toLong, dataArr(2).trim.toLong, dataArr(3).trim, dataArr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val processedStream: DataStream[String] = dataStream.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult()) //窗口聚合
      //按照窗口分组 计算排序
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

//    dataStream.print("inputData")
    processedStream.print("processedData")
    env.execute()

  }

  //自定义预聚合函数
  class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    //初始的累加器的值
    override def createAccumulator(): Long = 0

    //累加器
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    //数据的结果
    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  //自定义预聚合函数，计算平均数
//  class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
//    override def createAccumulator(): (Long, Int) = (0L, 0)
//
//    override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)
//
//    override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2
//
//    override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
//  }


  class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
    }
  }

  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    //定义list状态用来保存 一个窗口内的数据
    var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      itemState = getRuntimeContext.getListState( new ListStateDescriptor[ItemViewCount]("List-state",classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      //将每一次进来的数据保存在状态中
      itemState.add(value)
      //注册定时器
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    //触发定时器，对结果数据进行排序
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //将所有数据取出放到list集合中
      val allItems:ListBuffer[ItemViewCount] = new ListBuffer()
      import scala.collection.JavaConversions._
      for(item <- itemState.get()){
        allItems.add(item)
      }

      //按照count大小排序
      val sortedItem: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    itemState.clear()

      //将排名结果格式化输出
      val result = new StringBuilder
      result.append("时间: ").append(new Timestamp(timestamp -1)).append("\n")
      for(i <- sortedItem.indices){
        val currentItem = sortedItem(i)
        result.append("no").append(i + 1).append(": ")
          .append("商品ID=").append(currentItem.itemId)
          .append("浏览量=").append(currentItem.count)
          .append("\n")
      }
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }

}
