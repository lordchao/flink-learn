package com.learn.flink.process;

import com.learn.flink.bean.WaterSensor;
import com.learn.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;


public class TopNDemo {
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * 所有数据存到一个map key=vc value=count
     */
    public static void processApproach() throws Exception {
        env.setParallelism(1);
        env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L))
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new TopNAll()).print(); //并行度强制为1
        env.execute();
    }

    /**
     * 增量聚合+全量打标签
     */
    public static void aggApproach() throws Exception {
        env.setParallelism(1);
        env.socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ele, ts) -> ele.getTs() * 1000L))
                .keyBy(WaterSensor::getVc)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new VcCountAgg(), new WindowResult())
                .keyBy(r -> r.f2) //按照窗口结束时间keyBy
                .process(new TopN(2))
                .print();
        env.execute();

    }

    public static class TopNAll extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            Map<Integer, Integer> vcMap = new HashMap<>();
            //放到map计数
            for (WaterSensor ws : elements) {
                Integer vc = ws.getVc();
                if (vcMap.containsKey(vc)) vcMap.put(vc, vcMap.get(vc) + 1);
                else vcMap.put(vc, 1);
            }
            //放到list
            List<Tuple2<Integer, Integer>> data = new ArrayList<>();
            for (Integer vc : vcMap.keySet())
                data.add(Tuple2.of(vc, vcMap.get(vc)));
            //排序
            data.sort(new Comparator<Tuple2<Integer, Integer>>() {
                @Override
                public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                    return o2.f1 - o1.f1;
                }
            });
            StringBuilder str = new StringBuilder();
            str.append("================");
            str.append("\n");
            for (int i = 0; i < Math.min(2, data.size()); ++i) {
                Tuple2<Integer, Integer> vcCount = data.get(i);
                str.append("Top" + (i + 1));
                str.append("\n");
                str.append("vc=" + vcCount.f0);
                str.append("\n");
                str.append("count=" + vcCount.f1);
                str.append("\n");
                str.append("窗口结束时间=" + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS"));
                str.append("\n");
                str.append("================");
                str.append("\n");
            }
            out.collect(str.toString());
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {

        //不同窗口的统计结果
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> data;
        //要取的topn数量
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            data = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            //不同窗口分开
            Long windowEnd = value.f2;
            if (data.containsKey(windowEnd)) data.get(windowEnd).add(value);
            else {
                ArrayList<Tuple3<Integer, Integer, Long>> list = new ArrayList<>();
                list.add(value);
                data.put(windowEnd, list);
            }
            //注册定时器
            ctx.timerService().registerEventTimeTimer(windowEnd+1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Long windowEnd = ctx.getCurrentKey();
            // 1. 排序
            List<Tuple3<Integer, Integer, Long>> dataList = data.get(windowEnd);
            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    // 降序， 后 减 前
                    return o2.f1 - o1.f1;
                }
            });
            // 2. 取TopN
            StringBuilder outStr = new StringBuilder();

            outStr.append("================================\n");
            // 遍历 排序后的 List，取出前 threshold 个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
            for (int i = 0; i < Math.min(threshold, dataList.size()); i++) {
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                outStr.append("Top" + (i + 1) + "\n");
                outStr.append("vc=" + vcCount.f0 + "\n");
                outStr.append("count=" + vcCount.f1 + "\n");
                outStr.append("窗口结束时间=" + vcCount.f2 + "\n");
                outStr.append("================================\n");
            }

            // 用完的List，及时清理，节省资源
            dataList.clear();

            out.collect(outStr.toString());
        }

    }

    public static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return 0;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {

        @Override
        public void process(Integer Key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {

            Integer count = elements.iterator().next();
            Long windowEnd = context.window().getEnd();
            out.collect(Tuple3.of(Key, count, windowEnd));
        }
    }
    public static void main(String[] args) throws Exception {
        //processApproach();
        aggApproach();
    }
}


