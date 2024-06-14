package com.learn.flink.window;

import com.learn.flink.bean.WaterSensor;
import com.learn.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 全窗口函数
 * 窗口触发时统一计算
 */

public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override

                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        TimeWindow window = context.window();
                        long startTs = window.getStart(), endTs = window.getEnd();

                        String start = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String end = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long l = elements.spliterator().estimateSize();
                        out.collect("key ="+s+"的窗口["+start+","+end+"]包含"+l+"条数据===>"+elements);
                    }
                }).print();
        env.execute();

    }
}
