package com.learn.flink.sql;

import com.learn.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;

public class SQLDemo {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("1", 1L, 1),
                new WaterSensor("2", 2L, 2),
                new WaterSensor("3", 3L, 3)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //流转表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);
        tableEnv.createTemporaryView("sensor", table);

        Table sumTable = tableEnv.sqlQuery("select id, sum(vc) from sensor group by id");


        //创建映射表
        tableEnv.executeSql("CREATE TABLE MyUserTable (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3307/flink_training',\n" +
                "   'username'  = 'root' \n," +
                "   'password'  = 'password' \n," +
                "   'table-name' = 'user1'\n" +
                ");");

        tableEnv.executeSql("insert into MyUserTable values(3, 'a')");
        env.execute();
    }
}
