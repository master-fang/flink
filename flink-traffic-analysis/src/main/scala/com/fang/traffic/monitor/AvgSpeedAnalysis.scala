package com.fang.traffic.monitor

import java.util.Properties

import com.fang.traffic.constant.{AvgSpeedInfo, KafkaConstants, TrafficInfo}
import com.fang.traffic.sink.MysqlWriteDataSink
import com.fang.traffic.utils.KafkaPropertiesUtil
import com.msb.cityTraffic.utils.{AvgSpeedInfo, GlobalConstants, JdbcReadDataSource, MonitorInfo, TrafficInfo, WriteDataSink}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * 统计实时的平均车速，我设定一个滑动窗口，窗口长度是为5分钟，滑动步长为1分钟
 * 平均车速=当前窗口内通过车辆的车速之和 / 当前窗口内通过的车辆数量
 * 可能出现时间乱序问题，最长迟到5秒。
 */
object AvgSpeedAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)
    //创建一个Kafka的Source
    //    val stream: DataStream[TrafficInfo] = streamEnv.addSource(
    //      new FlinkKafkaConsumer[String](KafkaConstants.CAT_TRAFFIC_INFO_TOPIC, new SimpleStringSchema(), KafkaPropertiesUtil.getKafkaProperties(KafkaConstants.AVG_SPEED_MONITOR))
    //        .setStartFromEarliest() //从第一行开始读取数据
    //    )
    val stream: DataStream[TrafficInfo] = streamEnv.socketTextStream("localhost", 9999)
      .map(line => {
        var arr = line.split(",")
        TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      }) //引入Watermark，并且延迟时间为5秒
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) {
        override def extractTimestamp(element: TrafficInfo): Long = element.actionTime
      })

    stream.keyBy(_.monitorId)
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate( //设计一个累加器：二元组(车速之后，车辆的数量)
        new AggregateFunction[TrafficInfo, (Double, Long), (Double, Long)] {
          override def createAccumulator(): (Double, Long) = (0, 0)

          override def add(value: TrafficInfo, acc: (Double, Long)): (Double, Long) = {
            (acc._1 + value.speed, acc._2 + 1)
          }

          override def getResult(acc: (Double, Long)): (Double, Long) = acc

          override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = {
            (a._1 + b._1, a._2 + b._2)
          }
        },
        (k: String, w: TimeWindow, input: Iterable[(Double, Long)], out: Collector[AvgSpeedInfo]) => {
          val acc: (Double, Long) = input.last
          var avg: Double = (acc._1 / acc._2).formatted("%.2f").toDouble
          out.collect(AvgSpeedInfo(w.getStart, w.getEnd, k, avg, acc._2.toInt))
        }
      )
      .addSink(new MysqlWriteDataSink[AvgSpeedInfo](classOf[AvgSpeedInfo]))

    streamEnv.execute()

  }
}
