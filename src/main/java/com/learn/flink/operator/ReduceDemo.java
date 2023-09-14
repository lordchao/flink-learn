package com.learn.flink.operator;

import com.learn.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        KeyedStream<WaterSensor, String> waterSensorDataStreamSource = env
                .fromElements(new WaterSensor("s1", 1L, 1),
                        new WaterSensor("s1", 2L, 3),
                        new WaterSensor("s3", 3L, 4),
                        new WaterSensor("s3", 4L, 10))
                .keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor> reducer = waterSensorDataStreamSource.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.id, value2.ts, value1.vc + value2.vc);
            }
        });

        reducer.print();

        env.execute();

    }

    /**
     * 每个key的第一条数据来的时候不会进入reduce方法，会存到state然后直接输出
     * value1 和 value是滚动个更新的 value1是之前的计算结果 value2是现在来的数据
     */
}
