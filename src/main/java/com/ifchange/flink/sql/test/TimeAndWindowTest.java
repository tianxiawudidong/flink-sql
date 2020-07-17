package com.ifchange.flink.sql.test;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TimeAndWindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // map成样例类类型
//        DataStream<Sensor> dataStream = inputStream
//            .map(data -> {
//                Sensor sensor = new Sensor();
//                String[] split = data.split(",");
//                sensor.setId(split[0]);
//                return sensor;
//            }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(1)) {
//                @Override
//                public long extractTimestamp(Sensor element) {
//                    return element.getTime() * 1000;
//                }
//            });

        DataStream<Sensor> dataStream = inputStream
            .map(data -> {
                Sensor sensor = new Sensor();
                String[] split = data.split(",");
                sensor.setId(split[0]);
                return sensor;
            }).assignTimestampsAndWatermarks(new WatermarkStrategy<Sensor>() {

            @Override
            public WatermarkGenerator<Sensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Sensor>() {
                    private long maxTimestamp;
                    private long delay = 3000;

                    @Override
                    public void onEvent(Sensor event, long eventTimestamp, WatermarkOutput output) {
                        maxTimestamp = Math.max(maxTimestamp, event.getTime());
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp - delay));
                    }
                };
            }
        });

        // 将流转换成表，直接定义时间字段
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, temperature, timestamp.rowtime as ts");

        // 1. Table API
        // 1.1 Group Window聚合操作
        Table resultTable = sensorTable
            .window(Tumble.over("10.seconds").on("ts").as("tw"))
            .groupBy("id,tw")
            .select("id, id.count, tw.end");

        // 1.2 Over Window 聚合操作
        Table overResultTable = sensorTable
            .window(Over.partitionBy("id").orderBy("ts").preceding("2.rows").as("ow"))
            .select("id, ts, id.count over ow, temperature.avg over ow");

        // 2. SQL实现
        // 2.1 Group Windows
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery(
            "select id, count(id), hop_end(ts, interval '4' second, interval '10' second) from sensor group by id, hop(ts, interval '4' second, interval '10' second)");

        // 2.2 Over Window
        Table orderSqlTable = tableEnv.sqlQuery(
            "select id, ts, count(id)over w, avg(temperature)over w from sensor window w as(partition by id order by ts rows between 2 preceding and current row)"
        );
        //    sensorTable.printSchema()
        // 打印输出
        tableEnv.toRetractStream(resultTable, Row.class).print("");

        env.execute("time and window test job");

    }
}
