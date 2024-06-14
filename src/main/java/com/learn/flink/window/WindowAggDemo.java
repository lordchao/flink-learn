package com.learn.flink.window;

import com.learn.flink.bean.WaterSensor;
import com.learn.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;

/**
 * 窗口函数agg
 * 1. 增量聚合 来一条计算一条
 * 2. 输入，中间累加器，输出类型可以不一样
 * 3. 第一条数据来 会创建窗口 累加器
 */

public class WindowAggDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    //初始值
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }

                    @Override
                    //聚合逻辑
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("调用add方法");
                        return accumulator + value.getVc();
                    }

                    @Override
                    //窗口出发时输出最终结果
                    public String getResult(Integer accumulator) {
                        System.out.println("调用getResult方法");
                        return accumulator.toString();
                    }

                    @Override
                    //只用会话窗口才会用
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                }).print();
        env.execute();
    }
}
