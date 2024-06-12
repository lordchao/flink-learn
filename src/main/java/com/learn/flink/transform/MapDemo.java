package com.learn.flink.transform;

import com.learn.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        KeyedStream<WaterSensor, String> waterSensorDataStreamSource = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 3),
                new WaterSensor("s3", 3L, 4)).keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<String> res = waterSensorDataStreamSource.map(sensor -> sensor.id);

        res.print();
        env.execute();

    }
}
