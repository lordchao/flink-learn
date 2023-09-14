package com.learn.flink.functions;

import com.learn.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        String id = value.split(",")[0];
        Long ts = Long.parseLong(value.split(",")[1]);
        Integer vc = Integer.parseInt(value.split(",")[2]);
        return new WaterSensor(id, ts, vc);
    }
}
