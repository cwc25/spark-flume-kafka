package com.twq.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * 1、创建topic1和topic2
  * bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic1
  * bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 3 --partitions 1 --topic topic2
  * 2、查看topic
  * bin/kafka-topics.sh --describe --zookeeper master:2181 --topic topic1
  * *
  * 3、启动Spark Streaming程序
   spark-submit --class com.twq.streaming.kafka.DirectKafkaStreamSourceOffset \
   --master spark://master:7077 \
   --deploy-mode client \
   --driver-memory 512m \
   --executor-memory 512m \
   --total-executor-cores 4 \
   --executor-cores 2 \
   /home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
   master:9092,slave1:9092,slave2:9092 topic1,topic2

  4、模拟发送消息：bin/kafka-console-producer.sh --broker-list master:9092 --topic topic1
*/

object DirectKafkaStreamSourceOffset {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val ssc = new StreamingContext(sc, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    var offsetRanges = Array.empty[OffsetRange]

    directKafkaStream.print()

    directKafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>
        for (o <- offsetRanges) {
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
        rdd.take(10).foreach(println)
      }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
