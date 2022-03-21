package com.fang.traffic.constant

object KafkaConstants {
  /**
   * 实时车辆信息topic
   */
  val CAT_TRAFFIC_INFO_TOPIC: String = "flink_car_traffic_topic"
  val DECK_CAR_ANALYSIS_GROUP_ID: String = "deck_car_analysis"
  val AVG_SPEED_MONITOR: String = "avg_speed_monitor"
  val OUT_SPEED_MONITOR: String = "out_speed_monitor"
  val TOP_N_MONITOR: String = "top_n_monitor"
  /**
   * 实时违法车辆topic
   */
  val VIOLATION_CAT_INFO_TOPIC: String = "flink_violation_car_topic"
  val VIOLATION_CAT_POLICE_ACTION: String = "violation_car_action"
  /**
   * 车辆行驶轨迹信息
   */
  val CAR_DRIVER_TRACE_TOPIC: String = "flink_car_drive_trace_topic"
  val CAR_DRIVER_TRACE_ANALYSIS: String = "car_driver_trace_analysis"
}
