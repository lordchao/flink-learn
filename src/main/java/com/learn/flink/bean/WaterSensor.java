package com.learn.flink.bean;

import java.util.Objects;

public class WaterSensor {
    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    public String getId() {
        return id;
    }

    public Long getTs() {
        return ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setId(String id) {
        this.id = id;
    }

    public WaterSensor() {

    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }

    public String id;
    public Long ts;
    public Integer vc;

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }
}
