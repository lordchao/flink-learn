package com.learn.flink.transform;

import com.learn.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * map是一个返回值
 * flatmap没有返回值，返回值放到采集器中
 */

public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 11L, 13),
                new WaterSensor("s2", 13L, 121),
                new WaterSensor("s3", 5L, 421)
        );
        /**
         * 对于s1的数据是一进一出
         * 对于s2是一进多处
         * s3是1进0出
         */
        sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
                if ("s1".equals(waterSensor.getId()))
                    collector.collect(waterSensor.getVc().toString());
                else if ("s2".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getTs().toString());
                    collector.collect(waterSensor.getVc().toString());
                }
            }
        }).print();
        env.execute();
    }
}
