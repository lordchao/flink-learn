package com.learn.flink.window;

import com.learn.flink.bean.WaterSensor;
import com.learn.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 基于时间的窗口
 * 滚动窗口
 * 窗口之间没有重叠
 *
 * 滑动窗口 窗口长度和步长
 * 最近一个小时的指标每5秒输出一次，长度就是1小时 步长是5秒
 *
 * 会话窗口
 * 窗口间隔m，如果m时间内没有数据进入，上一个窗口就会结束
 *
 *
 * 基于计数的窗口
 * 没有会话窗口
 * 滑动窗口每经过一个步长都有一个输出
 */
public class WindowTypeDemo {
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //基于计数的窗口
    public static void countWindowDemo() throws Exception {
        env.setParallelism(1);
        env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                //.countWindow(5)/滚动
                .countWindow(5,2)//滑动
                .process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        GlobalWindow window = context.window();
                        long startTs = window.maxTimestamp();

                        String start = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long l = elements.spliterator().estimateSize();
                        out.collect("key ="+s+"的窗口["+start+"]包含"+l+"条数据===>"+elements);
                    }

                }).print();
        env.execute();
    }

    //基于时间的窗口
    public void timeWindow() throws Exception {
        env.setParallelism(1);
        env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) //滑动窗口
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) //滑动窗口
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
    public static void main(String[] args) throws Exception {
        //this.timeWindow();
        countWindowDemo();
    }
}
