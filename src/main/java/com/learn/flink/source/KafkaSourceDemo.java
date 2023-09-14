package com.learn.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9094")
                .setTopics("flink-learn")
                .setGroupId("learn")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest()).build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka")
                .print();

        env.execute();
    }
}

/**
 * kafka消费者
 * auto.reset.offsets 如果没有offset 从
 * earliest: 最早消费
 * latest: 最新消费
 */