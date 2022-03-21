package com.fang.traffic.warn

import com.fang.traffic.constant.{KafkaConstants, PoliceAction, ViolationInfo}
import com.fang.traffic.utils.KafkaPropertiesUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * 违法车辆和交警出警分析
 * 第一种，当前的违法车辆（在5分钟内）如果已经出警了。（最后输出道主流中做删除处理）。
 * 第二种，当前违法车辆（在5分钟后）交警没有出警（发出出警的提示，在侧流中发出）。
 * 第三种，有交警的出警记录，但是不是由监控平台报的警。
 * 需要两种数据流：
 * 1、系统的实时违法车辆的数据流
 * 2、交警实时出警记录数据
 */
object ViolationCarAndPoliceActionAnalysis {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    //    var stream1: DataStream[ViolationInfo] = streamEnv.addSource(new FlinkKafkaConsumer[String](KafkaConstants.VIOLATION_CAT_INFO_TOPIC
    //      , new SimpleStringSchema(), KafkaPropertiesUtil.getKafkaProperties(KafkaConstants.VIOLATION_CAT_POLICE_ACTION)))
    //      .map(data => {
    //        val arr: Array[String] = data.split(",")
    //        ViolationInfo(arr(0), arr(1), arr(2).toLong)
    //      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ViolationInfo](Time.seconds(2)) {
    //      override def extractTimestamp(element: ViolationInfo): Long = element.createTime
    //    })
    val stream1: DataStream[ViolationInfo] = streamEnv.socketTextStream("localhost", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        ViolationInfo(arr(0), arr(1), arr(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ViolationInfo](Time.seconds(5)) {
        override def extractTimestamp(element: ViolationInfo): Long = element.createTime
      })

    val stream2: DataStream[PoliceAction] = streamEnv.socketTextStream("localhost", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        PoliceAction(arr(0), arr(1), arr(2), arr(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PoliceAction](Time.seconds(5)) {
      override def extractTimestamp(element: PoliceAction): Long = element.actionTime
    })

    val tag1 = new OutputTag[PoliceAction]("Not Found Car!")
    val tag2 = new OutputTag[ViolationInfo]("Please Action!")

    var mainStream = stream1.keyBy(_.car).connect(stream2.keyBy(_.car))
      .process(new KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String] {
        //需要两个状态，分别保存违法数据，出警记录
        lazy val vState: ValueState[ViolationInfo] = getRuntimeContext.getState(new ValueStateDescriptor[ViolationInfo]("v", classOf[ViolationInfo]))
        lazy val pState: ValueState[PoliceAction] = getRuntimeContext.getState(new ValueStateDescriptor[PoliceAction]("p", classOf[PoliceAction]))

        override def processElement1(value: ViolationInfo, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]): Unit = {
          val policeAction: PoliceAction = pState.value()
          if (policeAction == null) { //可能出警的数据还没有读到，或者该违法处理还没有交警出警
            ctx.timerService().registerEventTimeTimer(value.createTime + 5000) //5秒后触发提示
            vState.update(value)
          } else { //已经有一条与之对应的出警记录,可以关联
            out.collect(s"该违法车辆${value.car}，违法时间${value.createTime},已经有交警出警了，警号为:${policeAction.policeId},出警的状态是：${policeAction.actionStatus},出警的时间:${policeAction.actionTime}")
            vState.clear()
            pState.clear()
          }
        }

        //当从第二个流中读取一条出警记录数据
        override def processElement2(value: PoliceAction, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#Context, out: Collector[String]): Unit = {
          val info: ViolationInfo = vState.value()
          if (info == null) { //出警记录没有找到对应的违法车辆信息
            ctx.timerService().registerEventTimeTimer(value.actionTime + 5000)
            pState.update(value)
          } else { //已经有一条与之对应的出警记录,可以关联
            out.collect(s"该违法车辆${info.car}，违法时间${info.createTime},已经有交警出警了，警号为:${value.policeId},出警的状态是：${value.actionStatus},出警的时间:${value.actionTime}")
            vState.clear()
            pState.clear()
          }
        }

        //触发器触发的函数
        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, ViolationInfo, PoliceAction, String]#OnTimerContext, out: Collector[String]): Unit = {
          val info: ViolationInfo = vState.value()
          val action: PoliceAction = pState.value()
          if (info == null && action != null) { //表示有出警记录，但是没有匹配的违法车辆
            ctx.output(tag1, action)
          }
          if (action == null && info != null) { //有违法车辆信息，但是5分钟内还没有出警记录
            ctx.output(tag2, info)
          }
          //清空状态
          pState.clear()
          vState.clear()
        }
      })

    mainStream.print()
    mainStream.getSideOutput(tag1).print("没有对应的违法车辆信息")
    mainStream.getSideOutput(tag2).print("该违法车辆在5分钟内没有交警出警")

    streamEnv.execute()

  }
}
