package com.learn.flink.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source1 = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 8888);

        // 各自处理各自的谁来了处理谁
        ConnectedStreams<String, String> connect = source1.connect(source2);
        connect.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                System.out.println("来源于数字流 "+value);
                return value;
            }

            @Override
            public String map2(String value) throws Exception {
                System.out.println("来源于字母流 " + value);
                return value;
            }
        }).print();
        
// 多并行度是如果使用process必须用keyby

        env.execute();
    }

}
