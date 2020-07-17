package com.ifchange.flink.sql.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> source = env.readTextFile("f://sensor.txt");

        DataStream<Sensor> sensorDataStream = source.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String s) throws Exception {
                Sensor sensor = new Sensor();
                String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, ",");
                sensor.setId(split[0]);
                sensor.setTime(Long.parseLong(split[1]));
                sensor.setTemparature(Double.parseDouble(split[2]));
                return sensor;
            }
        });

        Table sensor = tableEnv.fromDataStream(sensorDataStream);

//        Table select = sensor.select("id,time,temparature");

        Table select = sensor.select($("id"), $("time"), $("temparature"));

        tableEnv.toAppendStream(select, Row.class).print();

        env.execute("aa");

    }

}
