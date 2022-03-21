package com.fang.traffic.warn

import java.util.Properties

import com.fang.traffic.constant.{DeckCarWarning, KafkaConstants, TrafficInfo}
import com.fang.traffic.sink.MysqlWriteDataSink
import com.fang.traffic.utils.KafkaPropertiesUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 套牌车分析
 * 当某个卡口中出现一辆行驶的汽车，我们可以通过摄像头识别车牌号。然后在10秒内，
 * 另外一个卡口（或者当前卡口）也识别到了同样车牌的车辆，那么很有可能这两辆车之中有很大几率存在套牌车，
 * 因为一般情况下不可能有车辆在10秒内经过两个卡口
 *
 * 状态编程
 */
object DeckCarAnalysis {
  def main(args: Array[String]): Unit = {
    // 1.定义flink执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 2.kafka-source
    val prop: Properties = KafkaPropertiesUtil.getKafkaProperties(KafkaConstants.DECK_CAR_ANALYSIS_GROUP_ID)
    var kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](KafkaConstants.CAT_TRAFFIC_INFO_TOPIC, new SimpleStringSchema(), prop))
    // 3.operator
    // 3.1 parse data
    var stream: DataStream[TrafficInfo] = kafkaStream.map(data => {
      val arr = data.split(",")
      TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    }) //引入事件时间
      .assignAscendingTimestamps(_.actionTime)
      .uid("DeckCarAnalysis")
    // 3.2 handle
    stream.keyBy(_.car)
      .process(new KeyedProcessFunction[String, TrafficInfo, DeckCarWarning] {
        private var firstState: ValueState[TrafficInfo] = _

        override def open(parameters: Configuration): Unit = {
          val firstInfo = new ValueStateDescriptor[TrafficInfo]("firstInfo", classOf[TrafficInfo])
          firstState = getRuntimeContext.getState(firstInfo)
        }

        override def processElement(info: TrafficInfo, context: KeyedProcessFunction[String, TrafficInfo, DeckCarWarning]#Context, collector: Collector[DeckCarWarning]): Unit = {
          if (firstState.value() == null) {
            firstState.update(info)
          } else {
            // 判断事件是否超过10秒
            val actionTime = info.actionTime
            val firstTime = firstState.value().actionTime
            var less = (actionTime - firstTime).abs / 1000
            if (less <= 10) {
              var deck = new DeckCarWarning(info.car, if (actionTime > firstTime) firstState.value().monitorId else info.monitorId,
                if (actionTime > firstTime) info.monitorId else firstState.value().monitorId,
                "该车辆涉嫌套牌",
                context.timerService().currentProcessingTime()
              )
              collector.collect(deck)
              firstState.clear()
            } else if (actionTime > firstTime) {
              firstState.update(info)
            }
          }
        }
      })
      // 4.add sink
      .addSink(new MysqlWriteDataSink[DeckCarWarning](classType = classOf[DeckCarWarning]))

    env.execute("DeckCarAnalysis")

  }
}
