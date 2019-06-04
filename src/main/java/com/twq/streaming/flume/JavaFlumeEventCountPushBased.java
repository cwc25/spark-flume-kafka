package com.twq.streaming.flume;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 *
 * Flume-style Push-based Approach(Spark Streaming作为一个agent存在)
 *
 * 1、在slave1(必须要有spark的worker进程在)上启动一个flume agent
 * bin/flume-ng agent -n a1 -c conf -f conf/flume-conf.properties
 *
 * # Name the components on this agent
 * agent1.sources = r1
 * agent1.sinks = k1
 * agent1.channels = c1
 *
 * # Describe/configure the source
 * agent1.sources.r1.type = netcat
 * agent1.sources.r1.bind = localhost
 * agent1.sources.r1.port = 44445
 *
 * # Describe the sink
 * agent1.sinks.k1.type = logger
 *
 * # Use a channel that buffers events in memory
 * agent1.channels.c1.type = memory
 * agent1.channels.c1.capacity = 1000
 * agent1.channels.c1.transactionCapacity = 100
 *
 * # Bind the source and sink to the channel
 * agent1.sources.r1.channels = c1
 * agent1.sinks.k1.channel = c1
 * 
 *
 * 2、启动Spark Streaming应用
 spark-submit --class com.twq.streaming.flume.JavaFlumeEventCountPushBased \
 --master spark://master:7077 \
 --deploy-mode client \
 --driver-memory 512m \
 --executor-memory 512m \
 --total-executor-cores 4 \
 --executor-cores 2 \
 /home/hadoop-twq/spark-course/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
 slave1 44444

 3、在slave1上 telnet slave1 44445 发送消息
 */
public final class JavaFlumeEventCountPushBased {
    private JavaFlumeEventCountPushBased() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: JavaFlumeEventCountPushBased <host> <port>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        Duration batchInterval = new Duration(2000);
        SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCountPushBased");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
                FlumeUtils.createStream(ssc, host, port);

        flumeStream.count();

        flumeStream.count().map(in -> "Received " + in + " flume events.").print();

        ssc.start();
        ssc.awaitTermination();
    }
}
