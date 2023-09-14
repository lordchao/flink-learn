package com.learn.flink.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  1. RichFunction多了生命周期函数
 *  open 每个子任务启动时调用一次
 *  close 每个子任务结束时调用一次
 *  2. 多了一个运行时上下文
 *  3. 如果flink程序已成退出就不会调用close
 */

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStream<String> source = env.socketTextStream("localhost", 7777);
        source.map(new RichMapFunction<String, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                RuntimeContext runtimeContext = getRuntimeContext();
                String name = runtimeContext.getTaskName();
                int id = runtimeContext.getIndexOfThisSubtask();
                System.out.println("子任务编号=" + id + " 子任务名称="+name + "调用open()");
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                RuntimeContext runtimeContext = getRuntimeContext();
                String name = runtimeContext.getTaskName();
                int id = runtimeContext.getIndexOfThisSubtask();
                System.out.println("子任务编号=" + id + " 子任务名称="+name + "调用close()");

                super.close();
            }

            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value) + 1;
            }
        }).print();
        env.execute();
    }
}
