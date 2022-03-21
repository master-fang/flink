package com.fang.traffic.warn

import java.sql.DriverManager
import java.util.Properties

import com.fang.traffic.constant.{DangerousDrivingWarning, KafkaConstants, OutOfLimitSpeedInfo, TrafficInfo}
import com.fang.traffic.utils.KafkaPropertiesUtil
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

/**
 * 危险驾驶分析
 * 一辆机动车在2分钟内，超速通过卡口超过3次以上;而且每次超速的超过了规定速度的20%以上;
 * 这样的机动车涉嫌危险驾驶。系统需要实时找出这些机动车，并报警，追踪这些车辆的轨迹。
 * 注意：如果有些卡口没有设置限速值，可以设置一个城市默认限速。
 */
object DangerousDriverAnalysis {
  def main(args: Array[String]): Unit = {
    // 1.定义flink执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 2.kafka-source
    val prop: Properties = KafkaPropertiesUtil.getKafkaProperties(KafkaConstants.DECK_CAR_ANALYSIS_GROUP_ID)
    var kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](KafkaConstants.CAT_TRAFFIC_INFO_TOPIC, new SimpleStringSchema(), prop))
    // 3.处理消息
    var speedInfoStream: DataStream[OutOfLimitSpeedInfo] = kafkaStream.map(data => {
      val arr = data.split(",")
      TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    }).map(new MyRichMapFunction(60)) // 如果卡口没有限速,给默认值60，将原始对象转换为超速对象
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OutOfLimitSpeedInfo](Time.seconds(10)) {
        override def extractTimestamp(t: OutOfLimitSpeedInfo): Long = t.actionTime
      }).uid("speedInfoStream")
    // 4 cep 编程 复杂事件处理
    // 4.1 模式定义 分为单次执行模式和循环执行模式 这里选择循环执行模式 又分为设置循环次数 定义条件和模式序列（严格邻近、宽松邻近、非确定宽松邻近
    val pattern = Pattern.begin[OutOfLimitSpeedInfo]("begin").where(info => {
      info.realSpeed > info.limitSpeed * 1.2
    }).timesOrMore(3)
      .greedy
      .within(Time.minutes(2))
    // 4.2 模式检测-> // 4.3 选择结果
    val ps = CEP.pattern(speedInfoStream.keyBy(_.car), pattern)
    ps.select(func => {
      val list = func.get("begin").get.toList
      var sb = new StringBuilder("该车辆涉嫌危险驾驶,")
      var i = 1
      var sumSpeed = 0.0
      for (info <- list) {
        sb.append(s"第${i}个经过的卡口是:${info.monitorId}-->")
        i += 1
        sumSpeed += info.realSpeed
      }
      DangerousDrivingWarning(list.head.car, sb.toString(), System.currentTimeMillis(), sumSpeed / list.size)
    }).print()

    env.execute("DangerousDriverAnalysis")


  }

  /**
   * 这里自定义函数处理
   * 从mysql加载每个卡口的车速限制,最好的方式是从kafka读取 然后通过广播的形式广播出去
   * 参考：https://blog.csdn.net/tzs_1041218129/article/details/105283325
   *
   * @param speedLimit 默认的限制
   */
  class MyRichMapFunction(speedLimit: Int) extends RichMapFunction[TrafficInfo, OutOfLimitSpeedInfo] {
    var map: mutable.Map[String, Int] = scala.collection.mutable.Map[String, Int]()

    //一次性从数据库中读取所有卡口的限速
    override def open(parameters: Configuration): Unit = {
      var conn = DriverManager.getConnection("jdbc:mysql://localhost/traffic_monitor", "root", "123456")
      var pst = conn.prepareStatement("select monitor_id,speed_limit from t_monitor_info where speed_limit > 0")
      var set = pst.executeQuery()
      while (set.next()) {
        map.put(set.getString(1), set.getInt(2))
      }
      set.close()
      pst.close()
      conn.close()
    }


    override def map(in: TrafficInfo): OutOfLimitSpeedInfo = {
      //首先从Map集合中判断是否存在卡口的限速，如果不存在，默认限速为60
      val info = map.getOrElse(in.monitorId, speedLimit)
      OutOfLimitSpeedInfo(in.car, in.monitorId, in.roadId, in.speed, info, in.actionTime)
    }
  }

}
