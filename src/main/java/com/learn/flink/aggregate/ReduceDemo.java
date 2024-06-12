package com.learn.flink.aggregate;

import com.learn.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 1.keyby之后调用 所以key会决定是否进入相同的reduce
 * 2.输入类型 = 输出类型
 * 3.第一条数据不会执行reduce方法，会存起来
 * 4.reduce方法中的两个参数第一个是上次的计算结果，第二个是当前数据
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s1", 13L, 21),
                new WaterSensor("s2", 5L, 2),
                new WaterSensor("s3", 5L, 3)
        );

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {

            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });
        SingleOutputStreamOperator<WaterSensor> reduce = waterSensorStringKeyedStream
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor w1, WaterSensor w2) throws Exception {
                        System.out.println("value1: " + w1);
                        System.out.println("value2: " + w2);
                        return new WaterSensor(w1.id, w1.ts, w1.vc + w2.vc);
                    }
                });
        reduce.print();
        env.execute();
    }
}
