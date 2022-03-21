package com.fang.traffic.utils

import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer

object KafkaPropertiesUtil {

  def getKafkaProperties(group: String): Properties = {
    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    prop.setProperty("group.id", group)
    prop.setProperty("key.deserializer", classOf[StringSerializer].getName)
    prop.setProperty("value.deserializer", classOf[StringSerializer].getName)
    prop
  }

}
