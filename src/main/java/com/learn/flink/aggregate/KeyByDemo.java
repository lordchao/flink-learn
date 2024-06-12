package com.learn.flink.aggregate;

import com.learn.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 1. Keyby不是转换算子 返回的是键控流 不能设置并行度
 * 2. 分组和分区的关系
 *  keyby是对数据分子 保证相同key的数据在同一个分区
 *  一字任务可以理解为一个分区，一个分区中可以存在多个分组(key)
 */

public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 11L, 13),
                new WaterSensor("s2", 13L, 121),
                new WaterSensor("s4", 5L, 421)
        );

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });
        waterSensorStringKeyedStream.flatMap(new FlatMapFunction<WaterSensor, Object>() {

            @Override
            public void flatMap(WaterSensor waterSensor, Collector<Object> collector) throws Exception {
                collector.collect(waterSensor.id);
            }
        }).print();
        env.execute();
    }
}
