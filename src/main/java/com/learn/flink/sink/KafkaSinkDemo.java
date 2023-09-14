package com.learn.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 如果需要开启精准一次需要满足下列条件
 * 1. 开启checkpoint
 * 2. 开启事务前缀
 * 3. 设置事务超时时间
 */

public class KafkaSinkDemo {
    public void sinkWithKey () {

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //delivery guarantee exactly once 必须开启checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStream<String> ds = env.socketTextStream("localhost", 7777);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9094")
                //Kafka sink 的topic要在record 序列化器中指定
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .<String>builder()
                                .setTopic("flink-learn")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink-learn")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*1000+"")
                .build();

        ds.sinkTo(sink);

        env.execute();


    }
}
